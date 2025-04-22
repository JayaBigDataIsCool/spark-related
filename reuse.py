# Import necessary libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import QuantileDiscretizer, Bucketizer
from typing import List, Dict # For type hinting
# import tempfile # No longer needed for base path
# import shutil # No longer needed for cleanup
import os # Still used for path joining if needed, but be careful with dbfs:/

# --- Databricks Setup ---
# NOTE: In Databricks, 'spark' session is pre-defined.
# dbutils is also pre-defined for interacting with the Databricks environment.
# Use dbutils.widgets for parameterization

# Define Widgets for Configuration
# These widgets will appear at the top of your Databricks notebook.
# You can set default values here.
try:
    dbutils.widgets.text("join_key_column", "user_id", "Join Key Column")
    dbutils.widgets.text("num_buckets", "10", "Number of Buckets")
    # Default to a temporary DBFS path. Change this to your preferred location.
    dbutils.widgets.text("base_output_path", "dbfs:/tmp/spark_partitioned_delta_dbx", "Base DBFS Output Path")
    dbutils.widgets.text("partition_key_col", "partition_key", "Partition Key Column Name")
except:
    print("WARNING: dbutils not found. Running outside Databricks or dbutils is unavailable.")
    print("Using hardcoded defaults instead of widgets.")

# --- Configuration (Retrieved from Widgets) ---
# Retrieve values from widgets. If widgets failed, use defaults.
try:
    JOIN_KEY_COLUMN = dbutils.widgets.get("join_key_column")
    # Ensure num_buckets is an integer
    NUM_BUCKETS = int(dbutils.widgets.get("num_buckets"))
    BASE_OUTPUT_PATH = dbutils.widgets.get("base_output_path")
    PARTITION_KEY_COL = dbutils.widgets.get("partition_key_col")
except NameError: # Handle case where dbutils wasn't found
    JOIN_KEY_COLUMN = "user_id"
    NUM_BUCKETS = 10
    BASE_OUTPUT_PATH = "/tmp/spark_partitioned_delta_local" # Fallback for local execution
    PARTITION_KEY_COL = "partition_key"
    print(f"Using fallback configuration: JOIN_KEY_COLUMN='{JOIN_KEY_COLUMN}', NUM_BUCKETS={NUM_BUCKETS}, BASE_OUTPUT_PATH='{BASE_OUTPUT_PATH}'")


print(f"--- Configuration ---")
print(f"Join Key Column: {JOIN_KEY_COLUMN}")
print(f"Number of Buckets: {NUM_BUCKETS}")
print(f"Base Output Path: {BASE_OUTPUT_PATH}")
print(f"Partition Key Column: {PARTITION_KEY_COL}")


# --- Sample Data Creation ---
# (Assuming 'spark' session is available from Databricks)
# Create sample dataframes to simulate your tables

# Master Table DataFrame (e.g., user profiles) - potentially larger
master_data = [
    (101, "Alice", 30), (102, "Bob", 25), (103, "Charlie", 35),
    (104, "David", 22), (105, "Eve", 28), (106, "Frank", 40),
    (107, "Grace", 31), (108, "Heidi", 29), (109, "Ivan", 27),
    (110, "Judy", 33), (111, "Mallory", 45), (112, "Niaj", 20),
    (113, "Olivia", 38), (114, "Peggy", 26), (115, "Quentin", 32)
] * 10 # Multiply to make it a bit larger for discretization
master_df_obj = spark.createDataFrame(master_data, [JOIN_KEY_COLUMN, "name", "age"])

# Detail Table 1 DataFrame (e.g., user orders)
detail1_data = [
    (101, "order1", 50.0), (103, "order2", 25.5), (101, "order3", 10.0),
    (105, "order4", 100.0), (107, "order5", 75.2), (109, "order6", 15.0),
    (102, "order7", 30.0), (111, "order8", 200.0), (115, "order9", 45.0),
    (104, "order10", 60.0), (113, "order11", 90.0)
]
detail1_df_obj = spark.createDataFrame(detail1_data, [JOIN_KEY_COLUMN, "order_id", "amount"])

# Detail Table 2 DataFrame (e.g., user activity)
detail2_data = [
    (102, "login", "2023-10-26"), (104, "click", "2023-10-26"),
    (101, "view", "2023-10-27"), (105, "login", "2023-10-27"),
    (108, "logout", "2023-10-28"), (110, "view", "2023-10-28"),
    (112, "click", "2023-10-29"), (114, "login", "2023-10-29"),
    (106, "view", "2023-10-30"), (113, "click", "2023-10-30")
]
detail2_df_obj = spark.createDataFrame(detail2_data, [JOIN_KEY_COLUMN, "activity", "timestamp"])

print("\n--- Sample DataFrames Created (Objects) ---")
master_df_obj.show(5)
detail1_df_obj.show(5)
detail2_df_obj.show(5)


# --- Reusable Partitioning Function (DataFrame Input) ---
# This function remains largely the same, but relies on the calling environment
# for 'spark' and passes parameters like partition_key_col.

def create_partitioned_tables_from_dfs(
    spark: SparkSession, # Pass spark session explicitly
    master_df: DataFrame,
    detail_dfs: List[DataFrame],
    join_key_col: str,
    num_buckets: int,
    output_base_path: str,
    partition_key_col: str # Now passed as argument
) -> Dict[str, str]:
    """
    Analyzes join key distribution on master_df, creates consistent buckets,
    and writes partitioned master and detail DataFrames to Delta format.
    Optimized for Databricks (expects DBFS paths, uses provided spark session).

    Args:
        spark: The SparkSession (Delta enabled).
        master_df: The master DataFrame object for discretization.
        detail_dfs: A list of detail DataFrame objects to partition.
        join_key_col: The name of the column used for joining.
        num_buckets: The desired number of partitions/buckets.
        output_base_path: The base directory to write partitioned Delta tables (e.g., DBFS path).
        partition_key_col: The name for the new partition key column.

    Returns:
        A dictionary mapping generic table identifiers ('master', 'detail_0',
        'detail_1', etc.) to their corresponding output Delta table paths.
    """
    print(f"\n--- Starting Partitioning Process from DataFrames for Key: '{join_key_col}' ---")

    # 1. Analyze Key Distribution on Master DataFrame
    print(f"1. Analyzing key distribution on master DataFrame...")
    if join_key_col not in master_df.columns:
         raise ValueError(f"Join key column '{join_key_col}' not found in master DataFrame.")
    if dict(master_df.dtypes)[join_key_col] not in ('int', 'bigint', 'double', 'float', 'long'):
         raise TypeError(f"Join key column '{join_key_col}' must be numeric for QuantileDiscretizer. Found type: {dict(master_df.dtypes)[join_key_col]}")

    qds = QuantileDiscretizer(
        numBuckets=num_buckets,
        inputCol=join_key_col,
        outputCol="temp_bucket_qds",
        relativeError=0.001,
        handleInvalid="error"
    )
    qds_model = qds.fit(master_df.select(join_key_col))

    # 2. Get Splits for Bucketizer
    splits = [-float("inf")] + qds_model.getSplits()[1:-1] + [float("inf")]
    print(f"2. Calculated {len(splits)-1} split points for {num_buckets} buckets: {splits}")

    # 3. Create Bucketizer
    print(f"3. Creating Bucketizer with these splits...")
    bucketizer = Bucketizer(
        splits=splits,
        inputCol=join_key_col,
        outputCol=partition_key_col, # Use the parameter
        handleInvalid="error"
    )

    # Combine master and detail dfs for processing
    all_dfs_to_process = {'master': master_df}
    for i, df in enumerate(detail_dfs):
        if join_key_col not in df.columns:
            print(f"WARNING: Join key column '{join_key_col}' not found in detail DataFrame at index {i}. Skipping.")
            continue
        all_dfs_to_process[f'detail_{i}'] = df

    partitioned_dfs = {}
    output_paths = {}

    # 4. Apply Bucketizer and Prepare for Writing
    print(f"4. Applying Bucketizer to all DataFrames...")
    for identifier, df in all_dfs_to_process.items():
        print(f"   - Processing DataFrame: '{identifier}'")
        df_bucketed = bucketizer.transform(df)
        df_partitioned = df_bucketed.withColumn(
            partition_key_col,
            F.col(partition_key_col).cast("int")
        )
        partitioned_dfs[identifier] = df_partitioned
        # Construct the output path (using simple string concatenation for DBFS)
        output_paths[identifier] = f"{output_base_path}/{identifier}_partitioned_delta"
        print(f"     Schema for '{identifier}' after adding partition key:")
        df_partitioned.printSchema()
        df_partitioned.select(join_key_col, partition_key_col).show(5, truncate=False)

    # 5. Write Partitioned Tables in Delta Format
    print(f"\n5. Writing partitioned Delta tables to subdirectories in: '{output_base_path}'")
    for identifier, df_to_write in partitioned_dfs.items():
        output_path = output_paths[identifier]
        print(f"   - Writing '{identifier}' partitioned by '{partition_key_col}' to Delta path: {output_path}")
        try:
            # Ensure the target directory is clean before writing (important for overwrite)
            try:
                dbutils.fs.rm(output_path, recurse=True)
                print(f"     Cleaned existing path: {output_path}")
            except Exception as e:
                 # Handle cases where the path doesn't exist yet or other FS errors during cleanup
                 # Check if error message indicates path not found, which is okay
                 if "java.io.FileNotFoundException" not in str(e) and "Path does not exist" not in str(e):
                      print(f"     Warning during pre-write cleanup of {output_path}: {e}")
                 else:
                      print(f"     Path {output_path} does not exist yet, proceeding with write.")


            df_to_write.write \
                .partitionBy(partition_key_col) \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .save(output_path)
            print(f"     Successfully wrote Delta table '{identifier}'")
        except Exception as e:
            print(f"     ERROR writing Delta table '{identifier}': {e}")

    print("\n--- Partitioning Process Complete ---")
    return output_paths

# --- Execute the Partitioning with DataFrame Objects ---
# Pass parameters retrieved from widgets (or defaults)
delta_table_paths = create_partitioned_tables_from_dfs(
    spark=spark, # Use the Databricks spark session
    master_df=master_df_obj,
    detail_dfs=[detail1_df_obj, detail2_df_obj],
    join_key_col=JOIN_KEY_COLUMN,
    num_buckets=NUM_BUCKETS,
    output_base_path=BASE_OUTPUT_PATH,
    partition_key_col=PARTITION_KEY_COL # Pass partition key name
)

# --- Example: Reading and Joining Partitioned Delta Data ---
print("\n--- Example: Reading and Joining Partitioned Delta Data ---")

master_key = 'master'
detail1_key = 'detail_0' # Corresponds to detail1_df_obj

if master_key in delta_table_paths and detail1_key in delta_table_paths:
    master_partitioned_path = delta_table_paths[master_key]
    detail1_partitioned_path = delta_table_paths[detail1_key]

    # Check if paths actually exist before reading (optional but good practice)
    paths_exist = False
    try:
        dbutils.fs.ls(master_partitioned_path)
        dbutils.fs.ls(detail1_partitioned_path)
        paths_exist = True
    except Exception as e:
        print(f"WARNING: Cannot read tables, output paths may not exist. Error: {e}")

    if paths_exist:
        print(f"Reading Delta table '{master_key}' from: {master_partitioned_path}")
        df_master_read = spark.read.format("delta").load(master_partitioned_path)

        print(f"\nReading Delta table '{detail1_key}' from: {detail1_partitioned_path}")
        df_detail1_read = spark.read.format("delta").load(detail1_partitioned_path)

        print(f"\nSchema of read master Delta table:")
        df_master_read.printSchema()
        print(f"\nSchema of read detail 1 Delta table:")
        df_detail1_read.printSchema()

        print("\nPerforming join on partitioned Delta tables...")
        joined_df = df_master_read.join(
            df_detail1_read,
            on=JOIN_KEY_COLUMN,
            how="inner"
        )
        print("Showing joined results (first 10 rows):")
        joined_df.show(10)
        print("\nExplain plan for the join:")
        joined_df.explain()
    else:
         print("Skipping read/join as output paths were not confirmed to exist.")

else:
    print(f"Skipping read/join example because expected table paths ('{master_key}', '{detail1_key}') were not found in the returned dictionary: {delta_table_paths}")


# --- Cleanup Temporary Directory using dbutils ---
# Optional: You might want to keep the output tables.
# Add a widget to control cleanup?
# dbutils.widgets.dropdown("cleanup_output", "False", ["True", "False"], "Cleanup Output?")
# perform_cleanup = dbutils.widgets.get("cleanup_output") == "True"

perform_cleanup = True # Hardcoded to True for this example run

if perform_cleanup:
    print(f"\nCleaning up output directory: {BASE_OUTPUT_PATH}")
    try:
        # Use dbutils.fs.rm for DBFS paths
        dbutils.fs.rm(BASE_OUTPUT_PATH, recurse=True)
        print("Cleanup successful using dbutils.")
    except Exception as e:
        # Catch potential errors if dbutils is not available or path doesn't exist
        print(f"Error during dbutils cleanup {BASE_OUTPUT_PATH}: {e}")
        # Fallback attempt for local paths if dbutils failed (less likely in Databricks)
        # import shutil
        # if os.path.exists(BASE_OUTPUT_PATH) and not BASE_OUTPUT_PATH.startswith("dbfs:/"):
        #     try:
        #         shutil.rmtree(BASE_OUTPUT_PATH)
        #         print("Cleanup successful using shutil (local path).")
        #     except OSError as sh_e:
        #         print(f"Error removing directory using shutil {BASE_OUTPUT_PATH}: {sh_e}")
else:
    print(f"\nSkipping cleanup for: {BASE_OUTPUT_PATH}")


# spark.stop() # Definitely do not stop the SparkSession in Databricks notebooks
