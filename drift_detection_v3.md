### Executive Presentation: Architectural Design for Data Drift Detection in Insurance Ratemaking Dataset

---

#### Executive Summary
This presentation outlines a robust and cost-efficient architectural design for detecting data drift in a monthly-loaded insurance ratemaking dataset within Databricks Unity Catalog. The dataset, consisting of 500 columns and 900 million rows with approximately 50 critical data elements, supports critical premium calculations and regulatory compliance. Ensuring data quality, integrity, and the detection of anomalies or fraud is paramount to prevent financial losses and regulatory risks. Leveraging Databricks Lakehouse Monitoring, Delta Lake time travel, and targeted column monitoring, this solution addresses the dataset’s scale, complexity, and varying granularity while optimizing costs. It evaluates multiple approaches across different data loading scenarios (appended partitions, separate tables, truncate-and-overwrite), providing a detailed cost analysis, optimization strategies, and an experimentation plan to ensure alignment with business objectives and financial efficiency as of April 24, 2025.

#### Objectives
- Ensure data quality and integrity for accurate ratemaking calculations.
- Detect data drift between monthly datasets to identify anomalies or fraud.
- Manage a large, complex dataset (500 columns, 900 million rows) with 50 critical data elements.
- Support flexible data loading scenarios (partitions, separate tables, overwrite).
- Optimize costs while maintaining scalability and accuracy.
- Provide a clear implementation and experimentation plan for executive decision-making.

#### Business Context
The insurance ratemaking dataset underpins premium calculations by analyzing policy details (e.g., premiums, coverage types), claim data (e.g., loss amounts, claim frequencies), risk factors (e.g., geographic location, driver age), and derived metrics (e.g., loss ratios). The 50 critical data elements, encompassing key facts and dimensions such as premium amounts and loss ratios, directly influence rate-setting accuracy. The dataset’s varying granularity—spanning policy-level, claim-level, and aggregated data—requires a solution that can detect drift across all levels to ensure reliable ratemaking. Data drift in these elements could lead to mispriced premiums, financial losses, or regulatory non-compliance, while anomalies may indicate fraud or data quality issues. This solution balances comprehensive monitoring with cost efficiency, prioritizing business-critical elements while ensuring scalability for the dataset’s scale.

#### System Architecture Overview
The architecture is deployed within Databricks Unity Catalog, utilizing Delta Lake for storage and Lakehouse Monitoring for automated drift detection. It supports three data loading scenarios:
- **Append with Partitions**: Data added to a single table with monthly partitions.
- **Separate Tables**: New table created each month (e.g., `ratemaking_data_2025_01`).
- **Truncate and Overwrite**: Table overwritten monthly, retaining only current data.

**Components**:
- **Data Ingestion**: Processes for loading monthly datasets into Delta tables.
- **Data Storage**: Delta Lake tables optimized for scale and complexity.
- **Drift Detection**: Lakehouse Monitoring, time travel, or custom PySpark, with options for critical column monitoring.
- **Monitoring and Alerting**: Dashboards and SQL alerts for actionable insights.
- **Governance and Security**: Unity Catalog controls and audit logs for compliance.

#### Data Ingestion and Storage
- **Delta Lake Table**:
  - Primary table (`ratemaking_data`) stored as a Delta table in Unity Catalog.
  - For partitioned setups, use year and month partitioning to optimize query performance:
    ```sql
    CREATE TABLE ratemaking_data
    (col1 STRING, ..., premium_amount DOUBLE, loss_ratio DOUBLE, load_date TIMESTAMP)
    USING DELTA
    PARTITIONED BY (year INT, month INT);
    ```
  - Include a `load_date` timestamp column to enable Time Series profiles, ensuring consistent monthly load tracking across granularities:
    ```sql
    SELECT *, current_timestamp() AS load_date FROM source_data
    ```
- **Retention Settings**:
  - Configure retention to support time travel for at least 30 days, critical for overwrite scenarios:
    ```sql
    ALTER TABLE ratemaking_data SET TBLPROPERTIES (
      'delta.logRetentionDuration' = 'interval 30 days',
      'delta.deletedFileRetentionDuration' = 'interval 30 days'
    );
    ```
  - Verify settings:
    ```sql
    SHOW TBLPROPERTIES ratemaking_data
    ```
- **Vacuuming**:
  - Prevent data loss by setting a safe vacuuming retention period:
    ```sql
    VACUUM ratemaking_data RETAIN 720 HOURS;
    ```
- **Optimization**:
  - Optimize for 900 million rows to enhance query performance:
    ```sql
    OPTIMIZE ratemaking_data ZORDER BY (premium_amount);
    ```

#### Drift Detection Mechanisms
The architecture evaluates six approaches to detect data drift, incorporating Time Series and Snapshot profiles, custom PySpark, and critical column monitoring. Each approach is designed to handle the dataset’s scale, complexity, and business-critical requirements.

##### 1. Time Series Profile on Single Partitioned Table
- **Description**: Monitors a single Delta table with monthly partitions, using a Time Series profile to compute drift between monthly windows based on the `load_date` column.
- **Implementation**:
  - Focus on 50 critical columns to optimize resources:
    ```sql
    CREATE MONITOR ratemaking_data_monitor
    ON TABLE ratemaking_data
    WITH TIME_SERIES (
      TIMESTAMP_COLUMN = 'load_date',
      TIME_WINDOW = '1 MONTH',
      COLUMNS = ['premium_amount', 'loss_ratio', ..., 'critical_column50']
    );
    ```
  - Optionally monitor all 500 columns for comprehensive coverage.
- **Metrics**: Generates drift metrics (e.g., Wasserstein distance, Jensen-Shannon divergence) for specified columns, stored in Delta tables.
- **Use Case**: Ideal for appended data with partitions, automating drift detection across varying granularities.

##### 2. Snapshot Profile on Single Partitioned Table
- **Description**: Uses a Snapshot profile on a partitioned table, comparing the current month’s data to a baseline table representing the previous month.
- **Implementation**:
  - Create baseline:
    ```sql
    CREATE TABLE ratemaking_data_baseline_2025_03 AS
    SELECT * FROM ratemaking_data WHERE year = 2025 AND month = 3;
    ```
  - Create monitor:
    ```sql
    CREATE MONITOR ratemaking_data_snapshot_monitor
    ON TABLE ratemaking_data
    WITH SNAPSHOT (
      BASELINE_TABLE = 'ratemaking_data_baseline_2025_03',
      SLICING_EXPRS = ['year = 2025 AND month = 4'],
      COLUMNS = ['premium_amount', 'loss_ratio', ..., 'critical_column50']
    );
    ```
- **Metrics**: Compares specified columns to the baseline.
- **Use Case**: Viable for partitioned tables but requires baseline management, less efficient than Time Series.

##### 3. Snapshot Profile on Separate Monthly Tables
- **Description**: Monitors separate tables created each month, using the previous month’s table as the baseline in a Snapshot profile.
- **Implementation**:
  - Create monitor:
    ```sql
    CREATE MONITOR ratemaking_data_2025_04_monitor
    ON TABLE ratemaking_data_2025_04
    WITH SNAPSHOT (
      BASELINE_TABLE = 'ratemaking_data_2025_03',
      COLUMNS = ['premium_amount', 'loss_ratio', ..., 'critical_column50']
    );
    ```
  - Automate with Databricks SDK for scalability:
    ```python
    from databricks.sdk.service.catalog import LakehouseMonitorsAPI
    client = LakehouseMonitorsAPI()
    client.create(
        table_name="ratemaking_data_2025_04",
        baseline_table_name="ratemaking_data_2025_03",
        profile_type="Snapshot",
        columns=['premium_amount', 'loss_ratio', ..., 'critical_column50']
    )
    ```
- **Metrics**: Drift metrics for specified columns.
- **Use Case**: Suitable for setups with separate monthly tables.

##### 4. Snapshot Profile for Truncate-and-Overwrite
- **Description**: Monitors a single table overwritten monthly, using a Snapshot profile with a baseline table created before overwriting.
- **Implementation**:
  - Create baseline:
    ```sql
    CREATE TABLE ratemaking_data_baseline_2025_03 AS SELECT * FROM ratemaking_data;
    ```
  - Create monitor:
    ```sql
    CREATE MONITOR ratemaking_data_monitor
    ON TABLE ratemaking_data
    WITH SNAPSHOT (
      BASELINE_TABLE = 'ratemaking_data_baseline_2025_03',
      COLUMNS = ['premium_amount', 'loss_ratio', ..., 'critical_column50']
    );
    ```
  - Time Travel Alternative (ad-hoc, less preferred):
    ```sql
    SELECT * FROM ratemaking_data TIMESTAMP AS OF '2025-03-31';
    ```
- **Metrics**: Drift metrics compared to baseline.
- **Use Case**: Necessary for overwrite scenarios, requiring careful baseline management.

##### 5. Custom PySpark Program
- **Description**: Implements custom drift detection logic using PySpark, focusing on critical columns or specific ratemaking metrics.
- **Implementation**:
  ```python
  from pyspark.sql import SparkSession
  from scipy.stats import ks_2samp
  spark = SparkSession.builder.appName("DriftDetection").getOrCreate()
  current_df = spark.sql("SELECT premium_amount, loss_ratio, ..., critical_column50 FROM ratemaking_data WHERE year=2025 AND month=4")
  previous_df = spark.sql("SELECT premium_amount, loss_ratio, ..., critical_column50 FROM ratemaking_data WHERE year=2025 AND month=3")
  for col in ['premium_amount', 'loss_ratio', ..., 'critical_column50']:
      current_values = current_df.select(col).rdd.flatMap(lambda x: x).collect()
      previous_values = previous_df.select(col).rdd.flatMap(lambda x: x).collect()
      ks_stat, p_value = ks_2samp(current_values, previous_values)
      print(f"KS Test for {col}: statistic={ks_stat}, p-value={p_value}")
  ```
- **Metrics**: Custom statistical tests tailored to ratemaking needs.
- **Use Case**: For business-specific logic not supported by Lakehouse Monitoring.

##### 6. Monitor Critical Columns
- **Description**: Monitors a view containing the 50 critical columns, applying any Lakehouse Monitoring profile.
- **Implementation**:
  - Create view:
    ```sql
    CREATE VIEW ratemaking_data_critical AS
    SELECT premium_amount, loss_ratio, ..., critical_column50, load_date
    FROM ratemaking_data;
    ```
  - Apply Time Series or Snapshot profile to the view, as described above.
- **Metrics**: Drift metrics for critical columns, reducing computational load.
- **Use Case**: Optimizes resources while focusing on ratemaking-critical data.

#### Cost Analysis and Justification
A detailed cost analysis ensures financial efficiency while maintaining the accuracy required for ratemaking. Costs are calculated based on Databricks’ pricing model for serverless SQL compute (Lakehouse Monitoring), interactive clusters (PySpark), and cloud storage (AWS S3), with assumptions validated against Databricks documentation ([Cost Management](https://docs.databricks.com/en/administration-guide/account-settings/cost-management.html)).

##### Cost Assumptions and Calculations
1. **Dataset Size**:
   - 900 million rows, 500 columns, average row size ~1KB (considering mixed data types: strings, doubles, timestamps).
   - Total uncompressed size: 900 million rows * 1KB/row = 900GB.
   - Delta Lake compression (typically 2-4x): ~225-450GB compressed.

2. **Storage Costs (AWS S3 Standard)**:
   - Rate: $0.023/GB/month.
   - Base table (225-450GB): $5.18-$10.35/month.
   - Baseline tables or time travel (if retained for 30 days): Doubles storage to 450-900GB, costing $10.35-$20.70/month.
   - Critical columns (50/500 = 10% of data): 22.5-45GB, costing $0.52-$1.04/month; with baseline, 45-90GB, costing $1.04-$2.07/month.

3. **Compute Costs (Serverless SQL for Lakehouse Monitoring)**:
   - Rate: $5/DBU (AWS serverless SQL pricing, conservative estimate).
   - Processing 900 million rows, 500 columns:
     - Full table: ~100-400 DBUs/month (based on processing time for statistical computations, assuming 1-4 hours at 100 DBUs/hour).
     - Cost: 100-400 DBUs * $5 = $500-$2,000/month.
   - Critical columns (10% of data): ~10-40 DBUs/month.
     - Cost: 10-40 DBUs * $5 = $50-$200/month.
   - Snapshot profiles (additional overhead for baseline comparison): ~20% higher, 120-480 DBUs/month for full table, costing $600-$2,400/month; 12-48 DBUs/month for critical columns, costing $60-$240/month.

4. **Compute Costs (Custom PySpark)**:
   - Interactive cluster: 200-600 DBUs/month (larger cluster, longer runtime for custom logic, 2-6 hours at 100 DBUs/hour).
   - Cost: 200-600 DBUs * $5 = $1,000-$3,000/month.

5. **Network Costs**:
   - Minimal (<$10/month), as data remains within Databricks, with negligible egress for dashboard access.

##### Estimated Monthly Costs
| **Approach** | **Compute Cost** | **Storage Cost** | **Total Cost** | **Optimization Potential** |
|--------------|------------------|------------------|----------------|---------------------------|
| **Time Series (Full)** | $500-$2,000 (100-400 DBUs) | $5-$21 (225-900GB) | $505-$2,021 | High (critical columns) |
| **Time Series (Critical)** | $50-$200 (10-40 DBUs) | $1-$2 (45-90GB) | $51-$202 | High |
| **Snapshot (Partitioned, Full)** | $600-$2,400 (120-480 DBUs) | $10-$21 (450-900GB) | $610-$2,421 | Medium |
| **Snapshot (Partitioned, Critical)** | $60-$240 (12-48 DBUs) | $1-$2 (45-90GB) | $61-$242 | High |
| **Snapshot (Separate, Full)** | $600-$2,400 (120-480 DBUs) | $10-$21 (450-900GB) | $610-$2,421 | Medium |
| **Snapshot (Separate, Critical)** | $60-$240 (12-48 DBUs) | $1-$2 (45-90GB) | $61-$242 | High |
| **Snapshot (Overwrite, Full)** | $600-$2,400 (120-480 DBUs) | $10-$21 (450-900GB) | $610-$2,421 | Medium |
| **Snapshot (Overwrite, Critical)** | $60-$240 (12-48 DBUs) | $1-$2 (45-90GB) | $61-$242 | High |
| **Custom PySpark** | $1,000-$3,000 (200-600 DBUs) | $5-$21 (225-900GB) | $1,005-$3,021 | Low |

##### Cost Optimization Strategies
1. **Monitor Critical Columns**:
   - Reduces compute and storage by 90%, targeting 50 critical columns (e.g., $51-$242/month for Time Series or Snapshot profiles).
   - Use views to isolate critical data, minimizing processing overhead.
2. **Optimize Table Structure**:
   - Partitioning and `ZORDER` reduce query scan times, lowering compute costs:
     ```sql
     OPTIMIZE ratemaking_data ZORDER BY (premium_amount);
     ```
3. **Manage Retention**:
   - Set `delta.deletedFileRetentionDuration` to 30 days, avoiding excess storage costs:
     ```sql
     ALTER TABLE ratemaking_data SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days');
     ```
   - Schedule vacuuming post-drift detection:
     ```sql
     VACUUM ratemaking_data RETAIN 720 HOURS;
     ```
4. **Automate Baseline Creation**:
   - For Snapshot profiles, automate baseline table creation to minimize redundant storage:
     ```python
     spark.sql("CREATE TABLE ratemaking_data_baseline_2025_03 AS SELECT * FROM ratemaking_data WHERE year = 2025 AND month = 3")
     ```
5. **Leverage Serverless Compute**:
   - Use Lakehouse Monitoring’s serverless model to avoid cluster management costs, more cost-effective than PySpark.
6. **Test with Smaller Datasets**:
   - Experiment with a subset (e.g., 1 million rows) to optimize configurations, reducing initial costs.

#### Pros and Cons Analysis
The following table evaluates each approach, emphasizing business alignment and cost efficiency, validated against Databricks documentation ([Lakehouse Monitoring Documentation](https://docs.databricks.com/en/lakehouse-monitoring/index.html)).

| **Approach** | **Data Setup** | **Monitor Type** | **Pros** | **Cons** | **Business Suitability** | **Cost Efficiency** |
|--------------|----------------|------------------|----------|----------|--------------------------|---------------------|
| **Time Series on Partitioned Table** | Single table, partitioned, timestamp | Time Series | - Automated monthly drift detection<br>- Scalable for 900M rows<br>- Handles complex granularity<br>- Low maintenance<br>- Serverless compute | - Requires timestamp and partitions<br>- Limited to built-in metrics | High: Ensures accurate ratemaking with minimal effort, ideal for ongoing monitoring. | High: $51-$202 with critical columns. |
| **Snapshot on Partitioned Table** | Single table, partitioned | Snapshot with baseline | - Supports partitioned tables<br>- Scalable<br>- Handles granularity via slicing<br>- Flexible column selection | - Requires baseline creation<br>- Less automated<br>- Higher maintenance | Medium: Viable but less efficient, suitable if partitions are used but automation is secondary. | Medium: $61-$242 with critical columns. |
| **Snapshot on Separate Tables** | Separate tables per month | Snapshot with baseline | - Straightforward for separate tables<br>- Scalable<br>- SDK automation possible<br>- Flexible for granularity | - Multiple tables increase storage<br>- Monitor setup per table<br>- Medium maintenance | Medium: Good for separate table setups, but less streamlined for ratemaking workflows. | Medium: $61-$242 with critical columns. |
| **Snapshot for Truncate-and-Overwrite** | Single table, overwrite | Snapshot with baseline | - Necessary for overwrites<br>- Scalable with baseline tables<br>- Supports critical columns | - High maintenance (baseline creation)<br>- Retention risks with time travel<br>- Less automated | Medium: Required for overwrites, but baseline management adds complexity. | Medium: $61-$242 with critical columns. |
| **Custom PySpark** | Any | Custom | - Highly flexible for ratemaking logic<br>- Can focus on critical columns<br>- Tailored metrics | - High development effort<br>- Maintenance intensive<br>- Higher compute costs | Low: Useful for specific needs, but less practical for routine monitoring. | Low: $1,005-$3,021. |
| **Critical Columns (Any Profile)** | View with critical columns | Any | - Reduces computational load<br>- Focuses on ratemaking-critical data<br>- Cost-effective<br>- Scalable | - Risks missing drift in non-critical columns<br>- Requires careful column selection | High: Optimizes resources while targeting key ratemaking factors, needs validation. | High: $51-$242 across profiles. |

#### Experimentation Plan
To select the optimal approach, conduct the following experiments on a test dataset (e.g., 1 million rows) to minimize costs:
1. **Time Series on Partitioned Table**:
   - Test monitoring 50 critical columns vs. all 500 columns.
   - Evaluate automation, performance, cost, and drift detection accuracy.
2. **Snapshot on Partitioned Table**:
   - Test with baseline table and slicing expressions, focusing on critical columns.
   - Compare setup effort and results to Time Series.
3. **Snapshot on Separate Tables**:
   - Create test tables for two months, automate monitor creation via SDK.
   - Assess storage costs and maintenance overhead.
4. **Snapshot for Truncate-and-Overwrite**:
   - Simulate overwrite with baseline table, test time travel as a fallback.
   - Evaluate complexity and cost implications.
5. **Critical Columns**:
   - Create a view with 50 critical columns, apply Time Series and Snapshot profiles.
   - Compare performance, cost, and drift detection with full table monitoring.
6. **Custom PySpark**:
   - Implement drift detection for critical columns.
   - Compare effort, cost, and scalability to Lakehouse Monitoring.

#### Monitoring and Alerting
- **Dashboards**: Customize Lakehouse Monitoring dashboards to highlight drift in critical columns, accessible via Databricks SQL ([Databricks Dashboards](https://docs.databricks.com/en/sql/user/dashboards/index.html)).
- **Alerts**: Configure SQL alerts for business-critical drift, notifying actuaries and data teams:
  ```sql
  CREATE ALERT ratemaking_data_drift_alert
  ON TABLE ratemaking_data_monitor_drift_metrics
  WHEN wasserstein_distance > 0.05
  NOTIFY 'actuary-team@example.com';
  ```
- **Performance**: Serverless compute ensures scalability for 900 million rows, validated for large datasets ([Lakehouse Monitoring Documentation](https://docs.databricks.com/en/lakehouse-monitoring/index.html)).

#### Security and Governance
- **Access Controls**:
  ```sql
  GRANT SELECT ON TABLE ratemaking_data TO actuaries;
  GRANT ALL PRIVILEGES ON MONITOR ratemaking_data_monitor TO data_engineers;
  ```
- **Audit Logs**: Track usage for regulatory compliance ([Audit Logs](https://docs.databricks.com/en/administration-guide/account-settings/audit-logs.html)).
- **Lineage**: Leverage Unity Catalog’s data lineage for transparency, critical for insurance regulations.

#### Implementation Details
- **Requirements**: Unity Catalog-enabled workspace, Databricks SQL access, Delta tables. Confirm regional support ([Region Support](https://docs.databricks.com/en/administration-guide/cloud-configurations/aws/regions.html)).
- **Testing**: Utilize example notebooks for Lakehouse Monitoring setup and validation ([Lakehouse Monitoring Documentation](https://docs.databricks.com/en/lakehouse-monitoring/index.html)).
- **Cost Monitoring**: Track compute and storage costs via Databricks cost management tools ([Cost Management](https://docs.databricks.com/en/administration-guide/account-settings/cost-management.html)).

#### Conclusion
This architectural design leverages Databricks Lakehouse Monitoring to deliver a scalable, cost-effective, and business-aligned solution for detecting data drift in the insurance ratemaking dataset. A Time Series profile on a single partitioned table, monitoring 50 critical columns, is recommended for its automation, efficiency, and low cost ($51-$202/month). Snapshot profiles are suitable for separate tables or overwrite scenarios, with similar cost benefits when optimized ($61-$242/month). Monitoring critical columns aligns with ratemaking priorities while minimizing expenses, but full monitoring ensures comprehensive coverage, validated through experimentation. By clarifying the data loading method, engaging business stakeholders to define critical columns, and conducting targeted experiments, this solution ensures data quality, regulatory compliance, and financial efficiency, meeting the expectations of senior leadership.

```sql
-- Create Delta table with partitioning
CREATE TABLE ratemaking_data
(col1 STRING, ..., premium_amount DOUBLE, loss_ratio DOUBLE, load_date TIMESTAMP)
USING DELTA
PARTITIONED BY (year INT, month INT);

-- Set retention settings for time travel
ALTER TABLE ratemaking_data SET TBLPROPERTIES (
  'delta.logRetentionDuration' = 'interval 30 days',
  'delta.deletedFileRetentionDuration' = 'interval 30 days'
);

-- Create baseline table for Snapshot profile
CREATE TABLE ratemaking_data_baseline_2025_03 AS
SELECT * FROM ratemaking_data WHERE year = 2025 AND month = 3;

-- Create view for critical columns
CREATE VIEW ratemaking_data_critical AS
SELECT premium_amount, loss_ratio, ..., critical_column50, load_date
FROM ratemaking_data;

-- Time Series profile (critical columns)
CREATE MONITOR ratemaking_data_monitor
ON TABLE ratemaking_data
WITH TIME_SERIES (
  TIMESTAMP_COLUMN = 'load_date',
  TIME_WINDOW = '1 MONTH',
  COLUMNS = ['premium_amount', 'loss_ratio', ..., 'critical_column50'],
  CUSTOM_METRICS = (
    SELECT avg(premium_amount) AS avg_premium FROM ratemaking_data
  )
);

-- Snapshot profile on partitioned table
CREATE MONITOR ratemaking_data_snapshot_monitor
ON TABLE ratemaking_data
WITH SNAPSHOT (
  BASELINE_TABLE = 'ratemaking_data_baseline_2025_03',
  SLICING_EXPRS = ['year = 2025 AND month = 4'],
  COLUMNS = ['premium_amount', 'loss_ratio', ..., 'critical_column50']
);

-- Snapshot profile on separate table
CREATE MONITOR ratemaking_data_2025_04_monitor
ON TABLE ratemaking_data_2025_04
WITH SNAPSHOT (
  BASELINE_TABLE = 'ratemaking_data_2025_03',
  COLUMNS = ['premium_amount', 'loss_ratio', ..., 'critical_column50']
);

-- SQL alert for drift
CREATE ALERT ratemaking_data_drift_alert
ON TABLE ratemaking_data_monitor_drift_metrics
WHEN wasserstein_distance > 0.05
NOTIFY 'actuary-team@example.com';

-- Optimize table
OPTIMIZE ratemaking_data ZORDER BY (premium_amount);

-- Vacuum with safe retention
VACUUM ratemaking_data RETAIN 720 HOURS;
```

#### Key References
- [Introduction to Databricks Lakehouse Monitoring](https://docs.databricks.com/en/lakehouse-monitoring/index.html)
- [Create a monitor using the API](https://docs.databricks.com/en/lakehouse-monitoring/create-monitor-api.html)
- [Databricks Dashboards](https://docs.databricks.com/en/sql/user/dashboards/index.html)
- [Audit Logs](https://docs.databricks.com/en/administration-guide/account-settings/audit-logs.html)
- [Cost Management](https://docs.databricks.com/en/administration-guide/account-settings/cost-management.html)
- [Region Support](https://docs.databricks.com/en/administration-guide/cloud-configurations/aws/regions.html)
