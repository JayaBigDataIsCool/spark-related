# Import necessary libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.ml.feature import QuantileDiscretizer, Bucketizer
from typing import List, Dict # For type hinting
import tempfile
import shutil
import os # Import os for path joining

# Create a SparkSession (if not already running in Databricks)
# Ensure Delta Lake support is enabled in your Spark configuration
spark = SparkSession.builder \
    .appName("DynamicPartitioningDeltaExampleDFInput") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# --- Configuration ---
JOIN_KEY_COLUMN = "user_id" # The column used for joining tables
NUM_BUCKETS = 10          # Number of partitions (buckets) to create
# Use a temporary directory for demonstration. Replace with your actual DBFS/S3/ADLS path.
BASE_OUTPUT_PATH = tempfile.mkdtemp(prefix="spark_partitioned_delta_df_")
print(f"Using temporary output path for Delta tables: {BASE_OUTPUT_PATH}")

# --- Sample Data Creation ---
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

print("--- Sample DataFrames Created (Objects) ---")
master_df_obj.show(5)
detail1_df_obj.show(5)
detail2_df_obj.show(5)


# --- Reusable Partitioning Function (DataFrame Input) ---

def create_partitioned_tables_from_dfs(
    spark: SparkSession, # SparkSession needed for context if not implicitly available
    master_df: DataFrame,
    detail_dfs: List[DataFrame],
    join_key_col: str,
    num_buckets: int,
    output_base_path: str,
    partition_key_col: str = "partition_key"
) -> Dict[str, str]:
    """
    Analyzes join key distribution on master_df, creates consistent buckets,
    and writes partitioned master and detail DataFrames to Delta format.

    Args:
        spark: The SparkSession (Delta enabled).
        master_df: The master DataFrame object for discretization.
        detail_dfs: A list of detail DataFrame objects to partition.
        join_key_col: The name of the column used for joining.
        num_buckets: The desired number of partitions/buckets.
        output_base_path: The base directory to write partitioned Delta tables.
        partition_key_col: The name for the new partition key column.

    Returns:
        A dictionary mapping generic table identifiers ('master', 'detail_0',
        'detail_1', etc.) to their corresponding output Delta table paths.
    """
    print(f"\n--- Starting Partitioning Process from DataFrames for Key: '{join_key_col}' ---")

    # 1. Analyze Key Distribution on Master DataFrame
    print(f"1. Analyzing key distribution on master DataFrame...")

    # Ensure join key exists and is numeric in the master DataFrame
    if join_key_col not in master_df.columns:
         raise ValueError(f"Join key column '{join_key_col}' not found in master DataFrame.")
    if dict(master_df.dtypes)[join_key_col] not in ('int', 'bigint', 'double', 'float', 'long'):
         raise TypeError(f"Join key column '{join_key_col}' must be numeric for QuantileDiscretizer. Found type: {dict(master_df.dtypes)[join_key_col]}")

    qds = QuantileDiscretizer(
        numBuckets=num_buckets,
        inputCol=join_key_col,
        outputCol="temp_bucket_qds", # Temporary column
        relativeError=0.001,
        handleInvalid="error"
    )

    # Fit the discretizer to the master DataFrame data
    qds_model = qds.fit(master_df.select(join_key_col))

    # 2. Get Splits for Bucketizer
    splits = [-float("inf")] + qds_model.getSplits()[1:-1] + [float("inf")]
    print(f"2. Calculated {len(splits)-1} split points for {num_buckets} buckets: {splits}")

    # 3. Create Bucketizer
    print(f"3. Creating Bucketizer with these splits...")
    bucketizer = Bucketizer(
        splits=splits,
        inputCol=join_key_col,
        outputCol=partition_key_col,
        handleInvalid="error" # Or 'keep'/'skip'
    )

    # Combine master and detail dfs for processing
    all_dfs_to_process = {'master': master_df}
    for i, df in enumerate(detail_dfs):
        # Basic validation
        if join_key_col not in df.columns:
            print(f"WARNING: Join key column '{join_key_col}' not found in detail DataFrame at index {i}. Skipping this DataFrame.")
            continue
        all_dfs_to_process[f'detail_{i}'] = df

    partitioned_dfs = {}
    output_paths = {} # Store paths for return value

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
        # Construct the full path for the Delta table
        output_paths[identifier] = os.path.join(output_base_path, f"{identifier}_partitioned_delta")
        print(f"     Schema for '{identifier}' after adding partition key:")
        df_partitioned.printSchema()
        df_partitioned.select(join_key_col, partition_key_col).show(5, truncate=False)

    # 5. Write Partitioned Tables in Delta Format
    print(f"\n5. Writing partitioned Delta tables to subdirectories in: '{output_base_path}'")
    for identifier, df_to_write in partitioned_dfs.items():
        output_path = output_paths[identifier]
        print(f"   - Writing '{identifier}' partitioned by '{partition_key_col}' to Delta path: {output_path}")
        try:
            df_to_write.write \
                .partitionBy(partition_key_col) \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") # Useful if schema changes slightly (e.g., adding partition col)
                .save(output_path) # Save as Delta table
            print(f"     Successfully wrote Delta table '{identifier}'")
        except Exception as e:
            print(f"     ERROR writing Delta table '{identifier}': {e}")

    print("\n--- Partitioning Process Complete ---")
    return output_paths # Return dict of output paths

# --- Execute the Partitioning with DataFrame Objects ---
delta_table_paths = create_partitioned_tables_from_dfs(
    spark=spark,
    master_df=master_df_obj,
    detail_dfs=[detail1_df_obj, detail2_df_obj], # Pass list of detail DataFrames
    join_key_col=JOIN_KEY_COLUMN,
    num_buckets=NUM_BUCKETS,
    output_base_path=BASE_OUTPUT_PATH
)

# --- Example: Reading and Joining Partitioned Delta Data ---
print("\n--- Example: Reading and Joining Partitioned Delta Data ---")

# Check if paths were returned successfully using the new identifiers
master_key = 'master'
detail1_key = 'detail_0' # Corresponds to detail1_df_obj

if master_key in delta_table_paths and detail1_key in delta_table_paths:
    master_partitioned_path = delta_table_paths[master_key]
    detail1_partitioned_path = delta_table_paths[detail1_key]

    print(f"Reading Delta table '{master_key}' from: {master_partitioned_path}")
    df_master_read = spark.read.format("delta").load(master_partitioned_path)

    print(f"\nReading Delta table '{detail1_key}' from: {detail1_partitioned_path}")
    df_detail1_read = spark.read.format("delta").load(detail1_partitioned_path)

    print(f"\nSchema of read master Delta table:")
    df_master_read.printSchema()

    print(f"\nSchema of read detail 1 Delta table:")
    df_detail1_read.printSchema()

    # Perform the join - Spark leverages partitioning if applicable
    print("\nPerforming join on partitioned Delta tables...")
    joined_df = df_master_read.join(
        df_detail1_read,
        on=JOIN_KEY_COLUMN, # Join on the original key
        how="inner"
    )

    print("Showing joined results (first 10 rows):")
    joined_df.show(10)

    print("\nExplain plan for the join:")
    joined_df.explain()
else:
    print(f"Skipping read/join example because expected table paths ('{master_key}', '{detail1_key}') were not found in the returned dictionary: {delta_table_paths}")


# --- Cleanup Temporary Directory ---
try:
    print(f"\nCleaning up temporary directory: {BASE_OUTPUT_PATH}")
    # Check if the directory exists before attempting removal
    if os.path.exists(BASE_OUTPUT_PATH):
         shutil.rmtree(BASE_OUTPUT_PATH)
         print("Cleanup successful.")
    else:
         print(f"Directory not found, skipping cleanup: {BASE_OUTPUT_PATH}")
except OSError as e:
    print(f"Error removing temporary directory {BASE_OUTPUT_PATH}: {e}")

# spark.stop() # Uncomment if running locally and not in Databricks/managed environment

