# Import necessary libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import QuantileDiscretizer, Bucketizer
from typing import List, Dict, Tuple # Added Tuple for type hinting
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
    # This widget now refers ONLY to the MASTER table's join key
    dbutils.widgets.text("master_join_key_column", "user_id", "Master Table Join Key Column")
    dbutils.widgets.text("num_buckets", "10", "Number of Buckets")
    # Default to a temporary DBFS path. Change this to your preferred location.
    dbutils.widgets.text("base_output_path", "dbfs:/tmp/spark_partitioned_delta_dbx_diffkeys", "Base DBFS Output Path")
    dbutils.widgets.text("partition_key_col", "partition_key", "Partition Key Column Name")
except:
    print("WARNING: dbutils not found. Running outside Databricks or dbutils is unavailable.")
    print("Using hardcoded defaults instead of widgets.")

# --- Configuration (Retrieved from Widgets) ---
# Retrieve values from widgets. If widgets failed, use defaults.
try:
    MASTER_JOIN_KEY_COLUMN = dbutils.widgets.get("master_join_key_column")
    # Ensure num_buckets is an integer
    NUM_BUCKETS = int(dbutils.widgets.get("num_buckets"))
    BASE_OUTPUT_PATH = dbutils.widgets.get("base_output_path")
    PARTITION_KEY_COL = dbutils.widgets.get("partition_key_col")
except NameError: # Handle case where dbutils wasn't found
    MASTER_JOIN_KEY_COLUMN = "user_id"
    NUM_BUCKETS = 10
    BASE_OUTPUT_PATH = "/tmp/spark_partitioned_delta_local_diffkeys" # Fallback for local execution
    PARTITION_KEY_COL = "partition_key"
    print(f"Using fallback configuration: MASTER_JOIN_KEY_COLUMN='{MASTER_JOIN_KEY_COLUMN}', NUM_BUCKETS={NUM_BUCKETS}, BASE_OUTPUT_PATH='{BASE_OUTPUT_PATH}'")


print(f"--- Configuration ---")
print(f"Master Join Key Column: {MASTER_JOIN_KEY_COLUMN}")
print(f"Number of Buckets: {NUM_BUCKETS}")
print(f"Base Output Path: {BASE_OUTPUT_PATH}")
print(f"Partition Key Column: {PARTITION_KEY_COL}")


# --- Sample Data Creation ---
# (Assuming 'spark' session is available from Databricks)
# Modify sample data to have DIFFERENT join key column names

# Master Table DataFrame (e.g., user profiles)
master_data = [
    (101, "Alice", 30), (102, "Bob", 25), (103, "Charlie", 35), (104, "David", 22),
    (105, "Eve", 28), (106, "Frank", 40), (107, "Grace", 31), (108, "Heidi", 29),
    (109, "Ivan", 27), (110, "Judy", 33), (111, "Mallory", 45), (112, "Niaj", 20),
    (113, "Olivia", 38), (114, "Peggy", 26), (115, "Quentin", 32)
] * 10
# Use the configured master join key name here
master_df_obj = spark.createDataFrame(master_data, [MASTER_JOIN_KEY_COLUMN, "name", "age"])

# Detail Table 1 DataFrame (e.g., user orders) - Use a DIFFERENT join key name: 'uid'
DETAIL1_JOIN_KEY_COLUMN = "uid" # Define the specific key name for this table
detail1_data = [
    (101, "order1", 50.0), (103, "order2", 25.5), (101, "order3", 10.0), (105, "order4", 100.0),
    (107, "order5", 75.2), (109, "order6", 15.0), (102, "order7", 30.0), (111, "order8", 200.0),
    (115, "order9", 45.0), (104, "order10", 60.0), (113, "order11", 90.0)
]
detail1_df_obj = spark.createDataFrame(detail1_data, [DETAIL1_JOIN_KEY_COLUMN, "order_id", "amount"])

# Detail Table 2 DataFrame (e.g., user activity) - Use the SAME name as master for this example: 'user_id'
DETAIL2_JOIN_KEY_COLUMN = MASTER_JOIN_KEY_COLUMN # Or could be different, e.g., "account_id"
detail2_data = [
    (102, "login", "2023-10-26"), (104, "click", "2023-10-26"), (101, "view", "2023-10-27"),
    (105, "login", "2023-10-27"), (108, "logout", "2023-10-28"), (110, "view", "2023-10-28"),
    (112, "click", "2023-10-29"), (114, "login", "2023-10-29"), (106, "view", "2023-10-30"),
    (113, "click", "2023-10-30")
]
detail2_df_obj = spark.createDataFrame(detail2_data, [DETAIL2_JOIN_KEY_COLUMN, "activity", "timestamp"])

print("\n--- Sample DataFrames Created (Objects with Potentially Different Key Names) ---")
print("Master DF Schema:")
master_df_obj.printSchema()
master_df_obj.show(5)
print("\nDetail DF 1 Schema:")
detail1_df_obj.printSchema()
detail1_df_obj.show(5)
print("\nDetail DF 2 Schema:")
detail2_df_obj.printSchema()
detail2_df_obj.show(5)


# --- Reusable Partitioning Function (DataFrame Input, Different Join Keys) ---

def create_partitioned_tables_from_dfs(
    spark: SparkSession,
    master_df: DataFrame,
    master_join_key_col: str, # Join key name for master_df
    detail_dfs_with_keys: List[Tuple[DataFrame, str]], # List of (detail_df, detail_join_key_name)
    num_buckets: int,
    output_base_path: str,
    partition_key_col: str
) -> Dict[str, str]:
    """
    Analyzes join key distribution on master_df, creates consistent buckets,
    and writes partitioned master and detail DataFrames to Delta format,
    handling potentially different join key column names across tables.

    Args:
        spark: The SparkSession (Delta enabled).
        master_df: The master DataFrame object for discretization.
        master_join_key_col: The name of the join key column in master_df.
        detail_dfs_with_keys: A list of tuples, where each tuple contains a
                               detail DataFrame and the name of its join key column,
                               e.g., [(df1, "key1"), (df2, "key2")].
        num_buckets: The desired number of partitions/buckets.
        output_base_path: The base directory to write partitioned Delta tables.
        partition_key_col: The name for the new partition key column.

    Returns:
        A dictionary mapping generic table identifiers ('master', 'detail_0', ...)
        to their corresponding output Delta table paths.
    """
    print(f"\n--- Starting Partitioning Process (Handling Different Join Keys) ---")
    print(f"Master Join Key: '{master_join_key_col}'")

    # 1. Analyze Key Distribution on Master DataFrame
    print(f"1. Analyzing key distribution on master DataFrame using '{master_join_key_col}'...")
    if master_join_key_col not in master_df.columns:
         raise ValueError(f"Master join key column '{master_join_key_col}' not found in master DataFrame.")
    if dict(master_df.dtypes)[master_join_key_col] not in ('int', 'bigint', 'double', 'float', 'long'):
         raise TypeError(f"Master join key column '{master_join_key_col}' must be numeric. Found type: {dict(master_df.dtypes)[master_join_key_col]}")

    qds = QuantileDiscretizer(
        numBuckets=num_buckets,
        inputCol=master_join_key_col, # Use master key here
        outputCol="temp_bucket_qds",
        relativeError=0.001,
        handleInvalid="error"
    )
    qds_model = qds.fit(master_df.select(master_join_key_col))

    # 2. Get Splits for Bucketizer (These splits are universal)
    splits = [-float("inf")] + qds_model.getSplits()[1:-1] + [float("inf")]
    print(f"2. Calculated {len(splits)-1} universal split points: {splits}")

    # --- Process Master Table ---
    print(f"3. Processing Master Table...")
    master_bucketizer = Bucketizer(
        splits=splits,
        inputCol=master_join_key_col, # Use master key name
        outputCol=partition_key_col,
        handleInvalid="error"
    )
    master_df_bucketed = master_bucketizer.transform(master_df)
    master_df_partitioned = master_df_bucketed.withColumn(
        partition_key_col,
        F.col(partition_key_col).cast("int")
    )

    partitioned_dfs = {'master': master_df_partitioned}
    output_paths = {'master': f"{output_base_path}/master_partitioned_delta"}

    print(f"     Schema for 'master' after adding partition key:")
    master_df_partitioned.printSchema()
    master_df_partitioned.select(master_join_key_col, partition_key_col).show(5, truncate=False)


    # --- Process Detail Tables ---
    print(f"\n4. Processing Detail Tables...")
    for i, (detail_df, detail_join_key_col) in enumerate(detail_dfs_with_keys):
        identifier = f'detail_{i}'
        print(f"   - Processing DataFrame: '{identifier}' using its join key '{detail_join_key_col}'")

        # Validate join key exists in this detail DataFrame
        if detail_join_key_col not in detail_df.columns:
            print(f"   WARNING: Join key column '{detail_join_key_col}' not found in detail DataFrame '{identifier}'. Skipping.")
            continue
        # Optional: Add numeric type check for detail key if needed, though bucketizer might handle some types implicitly
        # if dict(detail_df.dtypes)[detail_join_key_col] not in ('int', 'bigint', 'double', 'float', 'long'):
        #    print(f"   WARNING: Join key column '{detail_join_key_col}' in detail DataFrame '{identifier}' is not numeric. Bucketizer might fail.")


        # Create a specific Bucketizer instance for this DF using the universal splits
        detail_bucketizer = Bucketizer(
            splits=splits, # Use the SAME splits calculated from master
            inputCol=detail_join_key_col, # Use the SPECIFIC key name for this detail DF
            outputCol=partition_key_col,
            handleInvalid="error"
        )

        df_bucketed = detail_bucketizer.transform(detail_df)
        df_partitioned = df_bucketed.withColumn(
            partition_key_col,
            F.col(partition_key_col).cast("int")
        )

        partitioned_dfs[identifier] = df_partitioned
        output_paths[identifier] = f"{output_base_path}/{identifier}_partitioned_delta"
        print(f"     Schema for '{identifier}' after adding partition key:")
        df_partitioned.printSchema()
        # Show the specific join key used for this table alongside the partition key
        df_partitioned.select(detail_join_key_col, partition_key_col).show(5, truncate=False)


    # 5. Write Partitioned Tables in Delta Format
    print(f"\n5. Writing partitioned Delta tables to subdirectories in: '{output_base_path}'")
    for identifier, df_to_write in partitioned_dfs.items():
        output_path = output_paths[identifier]
        print(f"   - Writing '{identifier}' partitioned by '{partition_key_col}' to Delta path: {output_path}")
        try:
            # Ensure the target directory is clean before writing
            try:
                dbutils.fs.rm(output_path, recurse=True)
                print(f"     Cleaned existing path: {output_path}")
            except Exception as e:
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

# --- Execute the Partitioning with DataFrame Objects and Specific Keys ---
# Define the list of detail dataframes with their respective join key column names
detail_dfs_info = [
    (detail1_df_obj, DETAIL1_JOIN_KEY_COLUMN), # Using 'uid' for this one
    (detail2_df_obj, DETAIL2_JOIN_KEY_COLUMN)  # Using 'user_id' for this one
]

delta_table_paths = create_partitioned_tables_from_dfs(
    spark=spark,
    master_df=master_df_obj,
    master_join_key_col=MASTER_JOIN_KEY_COLUMN, # Pass master key name
    detail_dfs_with_keys=detail_dfs_info, # Pass list of (df, key_name) tuples
    num_buckets=NUM_BUCKETS,
    output_base_path=BASE_OUTPUT_PATH,
    partition_key_col=PARTITION_KEY_COL
)

# --- Example: Reading and Joining Partitioned Delta Data ---
# NOTE: When joining, you still need to alias the columns if they have different names!
print("\n--- Example: Reading and Joining Partitioned Delta Data ---")

master_key = 'master'
detail1_key = 'detail_0' # Corresponds to detail1_df_obj

if master_key in delta_table_paths and detail1_key in delta_table_paths:
    master_partitioned_path = delta_table_paths[master_key]
    detail1_partitioned_path = delta_table_paths[detail1_key]

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

        print("\nPerforming join on partitioned Delta tables (requires aliasing join keys)...")
        # IMPORTANT: Alias the join columns to match during the join operation
        joined_df = df_master_read.alias("m").join(
            df_detail1_read.alias("d1"),
            F.col(f"m.{MASTER_JOIN_KEY_COLUMN}") == F.col(f"d1.{DETAIL1_JOIN_KEY_COLUMN}"), # Use correct names!
            how="inner"
        ).select("m.*", "d1.order_id", "d1.amount") # Select desired columns

        print("Showing joined results (first 10 rows):")
        joined_df.show(10)
        print("\nExplain plan for the join:")
        joined_df.explain()
    else:
         print("Skipping read/join as output paths were not confirmed to exist.")
else:
    print(f"Skipping read/join example because expected table paths ('{master_key}', '{detail1_key}') were not found: {delta_table_paths}")


# --- Cleanup Temporary Directory using dbutils ---
perform_cleanup = True # Set default cleanup behavior

# Optional: Add a widget to control cleanup
# try:
#     dbutils.widgets.dropdown("cleanup_output", "True", ["True", "False"], "Cleanup Output?")
#     perform_cleanup = dbutils.widgets.get("cleanup_output") == "True"
# except:
#     print("Cleanup widget not available.")


if perform_cleanup:
    print(f"\nCleaning up output directory: {BASE_OUTPUT_PATH}")
    try:
        dbutils.fs.rm(BASE_OUTPUT_PATH, recurse=True)
        print("Cleanup successful using dbutils.")
    except Exception as e:
        print(f"Error during dbutils cleanup {BASE_OUTPUT_PATH}: {e}")
else:
    print(f"\nSkipping cleanup for: {BASE_OUTPUT_PATH}")

# spark.stop()
