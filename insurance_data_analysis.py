from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F
from pyspark.sql.functions import year, month, quarter, dayofmonth, ntile, datediff, when, col, lit, expr
from functools import reduce

# Initialize Spark Session - adjust config as needed
spark = SparkSession.builder \
    .appName("Insurance Data Analysis") \
    .getOrCreate()


# Helper function to create nicer tables with descriptions
def display_with_description(df, description):
    """Display a dataframe with a description header in Databricks"""
    print(f"\n=== {description} ===")
    display(df)

# 1. Basic DataFrame inspection and summary statistics
def explore_data(df):
    print("DataFrame Schema:")
    display(df.printSchema())
    
    print("\nBasic Statistics:")
    display(df.describe())
    
    print("\nPolicy Counts by State:")
    display(df.groupBy("acct_st_cd").count().orderBy(F.desc("count")))
    
    # Count of unique accounts
    unique_accounts = df.select("acct_nm").distinct().count()
    print(f"\nNumber of Unique Accounts: {unique_accounts}")
    
    # Count of unique policies
    unique_policies = df.select("pol_nbr").distinct().count()
    print(f"\nNumber of Unique Policies: {unique_policies}")

# 2. Time-based analysis - policies by effective date
def time_analysis(df):
    df_with_date = df.withColumn("eff_year", year("pol_eff_dt")) \
                    .withColumn("eff_month", month("pol_eff_dt"))
    
    print("Policy Distribution by Year and Month:")
    display(df_with_date.groupBy("eff_year", "eff_month") \
               .count() \
               .orderBy("eff_year", "eff_month"))
    
    # Policy duration analysis
    policy_duration = df.withColumn(
        "policy_duration_days", 
        datediff("pol_expir_dt", "pol_eff_dt")
    )
    
    print("Policy Duration Statistics:")
    display(policy_duration.select(
        F.min("policy_duration_days").alias("min_days"),
        F.max("policy_duration_days").alias("max_days"),
        F.avg("policy_duration_days").alias("avg_days"),
        F.expr("percentile(policy_duration_days, 0.5)").alias("median_days")
    ))
    
    return df_with_date

# 3. Premium analysis by policy form
def premium_analysis(df):
    premium_by_form = df.groupBy("pol_fm_cd") \
                        .agg(
                            F.count("rec_id").alias("policy_count"),
                            F.avg("tot_wprm_amt").alias("avg_written_premium"),
                            F.sum("tot_wprm_amt").alias("total_written_premium")
                        ) \
                        .orderBy(F.desc("total_written_premium"))
                        
    print("Premium Analysis by Policy Form:")
    display(premium_by_form)
    
    # Premium distribution statistics
    print("Premium Amount Distribution:")
    display(df.select(
        F.min("tot_wprm_amt").alias("min_premium"),
        F.expr("percentile(tot_wprm_amt, 0.25)").alias("25th_percentile"),
        F.expr("percentile(tot_wprm_amt, 0.5)").alias("median_premium"),
        F.expr("percentile(tot_wprm_amt, 0.75)").alias("75th_percentile"),
        F.max("tot_wprm_amt").alias("max_premium"),
        F.avg("tot_wprm_amt").alias("avg_premium"),
        F.stddev("tot_wprm_amt").alias("stddev_premium")
    ))
    
    return premium_by_form

# 4. Geographic distribution analysis
def geographic_analysis(df):
    state_premium = df.groupBy("acct_st_cd") \
                     .agg(
                         F.count("rec_id").alias("policy_count"),
                         F.sum("cy_yr1_ep_amt").alias("total_earned_premium"),
                         F.avg("cy_yr1_ep_amt").alias("avg_earned_premium")
                     ) \
                     .orderBy(F.desc("total_earned_premium"))
    
    print("Top States by Total Earned Premium:")
    display(state_premium)
    
    # State-City analysis
    if "cty_nm" in df.columns:
        print("Top Cities by Premium Volume:")
        display(df.groupBy("acct_st_cd", "cty_nm") \
          .agg(F.sum("tot_wprm_amt").alias("total_premium"), F.count("rec_id").alias("policy_count")) \
          .orderBy(F.desc("total_premium")) \
          .limit(15))
    
    return state_premium

# 5. Advanced Premium to Exposure ratio analysis by state
def premium_exposure_analysis(df):
    premium_exposure_ratio = df.withColumn(
        "premium_to_exposure_ratio", 
        F.when(F.col("tot_expos_qty") > 0, F.col("tot_wprm_amt") / F.col("tot_expos_qty")).otherwise(None)
    )
    
    state_ratios = premium_exposure_ratio.groupBy("acct_st_cd") \
                                        .agg(
                                            F.avg("premium_to_exposure_ratio").alias("avg_premium_per_exposure"),
                                            F.stddev("premium_to_exposure_ratio").alias("stddev_premium_per_exposure")
                                        ) \
                                        .orderBy(F.desc("avg_premium_per_exposure"))
    
    print("Premium to Exposure Ratio by State:")
    display(state_ratios)
    
    # Add exposure analysis by policy form
    form_exposure = premium_exposure_ratio.groupBy("pol_fm_cd") \
                                        .agg(
                                            F.sum("tot_expos_qty").alias("total_exposure"),
                                            F.avg("premium_to_exposure_ratio").alias("avg_premium_per_exposure"),
                                            F.count("rec_id").alias("policy_count")
                                        ) \
                                        .orderBy(F.desc("total_exposure"))
    
    print("Exposure Analysis by Policy Form:")
    display(form_exposure)
    
    return state_ratios

# 6. Time series analysis - quarterly trends
def quarterly_trends(df):
    df_quarterly = df.withColumn("eff_year", year("pol_eff_dt")) \
                    .withColumn("eff_quarter", quarter("pol_eff_dt"))
    
    quarterly_premiums = df_quarterly.groupBy("eff_year", "eff_quarter") \
                                   .agg(
                                       F.sum("tot_wprm_amt").alias("total_written_premium"),
                                       F.avg("tot_wprm_amt").alias("avg_written_premium"),
                                       F.count("rec_id").alias("policy_count")
                                   ) \
                                   .orderBy("eff_year", "eff_quarter")
    
    print("Quarterly Premium Trends:")
    display(quarterly_premiums)
    
    # Databricks visualization hint
    print("Visualization Tip: Use Plot Options to create a line chart with eff_year+eff_quarter as X axis and total_written_premium as Y axis")
    
    return quarterly_premiums

# 7. Industry analysis - top industries by premium
def industry_analysis(df):
    industry_summary = df.groupBy("indus_grp_nm") \
                         .agg(
                             F.count("rec_id").alias("policy_count"),
                             F.sum("tot_wprm_amt").alias("total_written_premium"),
                             F.avg("tot_wprm_amt").alias("avg_written_premium")
                         ) \
                         .orderBy(F.desc("total_written_premium"))
    
    print("Top Industries by Premium:")
    display(industry_summary)
    
    # Industry group code analysis
    print("Premium Analysis by Industry Group Code:")
    display(df.groupBy("indus_grp_cd") \
      .agg(
          F.count("rec_id").alias("policy_count"),
          F.sum("tot_wprm_amt").alias("total_premium"),
          F.avg("tot_wprm_amt").alias("avg_premium")
      ) \
      .orderBy(F.desc("total_premium")))
    
    return industry_summary

# 8. Visualization prep - create data for premium distribution
def premium_distribution(df):
    # Create deciles of premium amounts for visualization
    premium_deciles = df.select("rec_id", "tot_wprm_amt") \
                       .filter(F.col("tot_wprm_amt").isNotNull()) \
                       .withColumn("premium_decile", ntile(10).over(Window.orderBy("tot_wprm_amt")))
    
    decile_summary = premium_deciles.groupBy("premium_decile") \
                                  .agg(
                                      F.min("tot_wprm_amt").alias("min_premium"),
                                      F.max("tot_wprm_amt").alias("max_premium"),
                                      F.avg("tot_wprm_amt").alias("avg_premium"),
                                      F.count("rec_id").alias("policy_count")
                                  ) \
                                  .orderBy("premium_decile")
    
    print("Premium Distribution by Decile:")
    display(decile_summary)
    
    # Visualization tip for Databricks
    print("Visualization Tip: Use Plot Options to create a bar chart with premium_decile as X axis and policy_count as Y axis")
    
    return decile_summary

# 9. Commission analysis
def commission_analysis(df):
    commission_data = df.select(
        "acct_st_cd", 
        "pol_fm_cd", 
        "comm_pct", 
        "comm_amt", 
        "tot_wprm_amt"
    ).withColumn(
        "comm_ratio", 
        F.col("comm_amt") / F.col("tot_wprm_amt")
    )
    
    state_commission = commission_data.groupBy("acct_st_cd") \
                                     .agg(
                                         F.avg("comm_pct").alias("avg_commission_pct"),
                                         F.avg("comm_ratio").alias("avg_comm_to_premium_ratio"),
                                         F.sum("comm_amt").alias("total_commission")
                                     ) \
                                     .orderBy(F.desc("total_commission"))
    
    print("Commission Analysis by State:")
    display(state_commission)
    
    # Commission analysis by policy form
    form_commission = commission_data.groupBy("pol_fm_cd") \
                                   .agg(
                                       F.avg("comm_pct").alias("avg_commission_pct"),
                                       F.sum("comm_amt").alias("total_commission"),
                                       F.count("*").alias("policy_count")
                                   ) \
                                   .orderBy(F.desc("total_commission"))
    
    print("Commission Analysis by Policy Form:")
    display(form_commission)
    
    # Analysis of waived commissions
    if "waiv_comm_amt" in df.columns:
        waived_comm = df.filter(F.col("waiv_comm_amt") > 0) \
                      .groupBy("acct_st_cd") \
                      .agg(
                          F.sum("waiv_comm_amt").alias("total_waived_commission"),
                          F.count("*").alias("policies_with_waived_comm")
                      ) \
                      .orderBy(F.desc("total_waived_commission"))
        
        print("Waived Commission Analysis by State:")
        display(waived_comm)
    
    return state_commission

# 10. Year-over-year growth analysis
def yoy_growth_analysis(df):
    yoy_growth = df.select(
        "rec_id", 
        "cy_yr1_wprm_amt", 
        "cy_yr2_wprm_amt"
    ).withColumn(
        "yoy_growth_pct", 
        F.when(
            F.col("cy_yr2_wprm_amt") > 0,
            (F.col("cy_yr1_wprm_amt") - F.col("cy_yr2_wprm_amt")) / F.col("cy_yr2_wprm_amt") * 100
        ).otherwise(None)
    )
    
    growth_buckets = yoy_growth.withColumn(
        "growth_bucket",
        F.when(F.col("yoy_growth_pct") < -10, "Significant Decrease")
         .when(F.col("yoy_growth_pct").between(-10, -0.01), "Moderate Decrease")
         .when(F.col("yoy_growth_pct").between(0, 10), "Moderate Increase")
         .when(F.col("yoy_growth_pct") > 10, "Significant Increase")
         .otherwise("No Change")
    )
    
    growth_summary = growth_buckets.groupBy("growth_bucket") \
                                  .count() \
                                  .orderBy("growth_bucket")
    
    print("Year-over-Year Growth Analysis:")
    display(growth_summary)
    
    # Visualization tip for Databricks
    print("Visualization Tip: Use Plot Options to create a pie chart with growth_bucket as the key and count as the value")
    
    return growth_summary

# 11. Location-based risk analysis
def location_risk_analysis(df):
    # Analyze risk factors by location
    location_risk = df.groupBy("loc_nbr") \
                     .agg(
                         F.count("rec_id").alias("policy_count"),
                         F.avg("bas_rt").alias("avg_base_rate"),
                         F.sum("tot_wprm_amt").alias("total_premium")
                     ) \
                     .orderBy(F.desc("total_premium"))
    
    print("Location-based Risk Analysis:")
    display(location_risk.limit(10))
    
    return location_risk

# 12. Policy form and coverage type analysis
def coverage_analysis(df):
    # Analyze policies by coverage type
    coverage_summary = df.groupBy("pol_fm_cd") \
                        .pivot("cov_splt_typ_cd") \
                        .agg(F.count("rec_id")) \
                        .fillna(0)
    
    print("Coverage Type Distribution by Policy Form:")
    display(coverage_summary)
    
    return coverage_summary

# 13. NEW: Quarterly Premium Pattern Analysis
def quarterly_premium_patterns(df):
    """Analyze quarterly premium patterns to identify seasonality"""
    
    # First create a view with all quarterly premiums pivoted for easier analysis
    qtr_cols = [f"cy_qtr{i}_wprm_amt" for i in range(1, 9)]
    qtr_cols_exist = [col for col in qtr_cols if col in df.columns]
    
    if len(qtr_cols_exist) > 0:
        # Calculate quarterly percentage of annual premium
        quarterly_pattern = df.select(
            "rec_id", 
            *qtr_cols_exist,
            "cy_yr1_wprm_amt", 
            "cy_yr2_wprm_amt"
        )
        
        # Add percentage columns for each quarter
        for qtr_col in qtr_cols_exist:
            quarterly_pattern = quarterly_pattern.withColumn(
                f"{qtr_col}_pct",
                F.when(F.col("cy_yr1_wprm_amt") > 0, 
                       (F.col(qtr_col) / F.col("cy_yr1_wprm_amt")) * 100
                ).otherwise(None)
            )
        
        # Aggregate to get average quarterly patterns
        pct_cols = [f"{col}_pct" for col in qtr_cols_exist]
        
        print("Average Quarterly Premium Patterns:")
        display(quarterly_pattern.select(
            *[F.avg(col).alias(f"avg_{col}") for col in pct_cols]
        ))
        
        # Identify policies with significant quarterly variations
        variation_threshold = 25  # 25% variation between quarters
        
        for i in range(len(qtr_cols_exist) - 1):
            current_col = qtr_cols_exist[i]
            next_col = qtr_cols_exist[i + 1]
            
            quarterly_pattern = quarterly_pattern.withColumn(
                f"variation_{i+1}_to_{i+2}",
                F.abs((F.col(next_col) - F.col(current_col)) / F.when(F.col(current_col) > 0, F.col(current_col)).otherwise(1)) * 100
            )
        
        variation_cols = [col for col in quarterly_pattern.columns if col.startswith("variation_")]
        
        if variation_cols:
            print("Policies with Significant Quarterly Premium Variations:")
            display(quarterly_pattern.filter(
                reduce(lambda x, y: x | y, [F.col(col) > variation_threshold for col in variation_cols])
            ).select(
                "rec_id", *qtr_cols_exist, *variation_cols
            ).orderBy(F.desc(variation_cols[0])).limit(10))
        
        return quarterly_pattern

# 14. NEW: Vehicle Analysis for Auto Insurance
def vehicle_analysis(df):
    """Analyze vehicle-related data for auto insurance policies"""
    
    # Check if required vehicle columns exist
    vehicle_cols = ["vin", "veh_mk_mdl_nm_leg", "veh_cls_hgh_lvl_desc", "veh_grp_typ_cd"]
    vehicle_cols_exist = [col for col in vehicle_cols if col in df.columns]
    
    if len(vehicle_cols_exist) > 0:
        # Filter policies with vehicle information
        auto_policies = df.filter(F.col(vehicle_cols_exist[0]).isNotNull())
        
        if "veh_mk_mdl_nm_leg" in vehicle_cols_exist:
            # Extract make from the model name
            auto_policies = auto_policies.withColumn(
                "vehicle_make", 
                F.split(F.col("veh_mk_mdl_nm_leg"), " ").getItem(0)
            )
            
            print("Premium Analysis by Vehicle Make:")
            display(auto_policies.groupBy("vehicle_make") \
                       .agg(
                           F.count("rec_id").alias("policy_count"),
                           F.sum("tot_wprm_amt").alias("total_premium"),
                           F.avg("tot_wprm_amt").alias("avg_premium")
                       ) \
                       .orderBy(F.desc("policy_count")) \
                       .limit(15))
        
        # Vehicle class analysis
        if "veh_cls_hgh_lvl_desc" in vehicle_cols_exist:
            print("Premium Analysis by Vehicle Class:")
            display(auto_policies.groupBy("veh_cls_hgh_lvl_desc") \
                       .agg(
                           F.count("rec_id").alias("policy_count"),
                           F.sum("tot_wprm_amt").alias("total_premium"),
                           F.avg("tot_wprm_amt").alias("avg_premium")
                       ) \
                       .orderBy(F.desc("total_premium")) \
                       .limit(10))
        
        # Vehicle group type analysis
        if "veh_grp_typ_cd" in vehicle_cols_exist:
            print("Premium Analysis by Vehicle Group Type:")
            display(auto_policies.groupBy("veh_grp_typ_cd") \
                       .agg(
                           F.count("rec_id").alias("policy_count"),
                           F.sum("tot_wprm_amt").alias("total_premium"),
                           F.avg("tot_wprm_amt").alias("avg_premium")
                       ) \
                       .orderBy(F.desc("policy_count")) \
                       .limit(10))
        
        return auto_policies

# 15. NEW: Risk Factor Analysis
def risk_factor_analysis(df):
    """Analyze premium correlations with various risk factors"""
    
    # Check if required risk columns exist
    risk_cols = [
        "haz_cd_leg", "bas_rt", "rpt_mnr_ln_cd", "rtmk_prdct_cd_leg", 
        "waiv_rsn_cd_leg", "nprm_cd_inp", "prem_typ_cd"
    ]
    risk_cols_exist = [col for col in risk_cols if col in df.columns]
    
    if len(risk_cols_exist) > 0:
        # Analyze by hazard code
        if "haz_cd_leg" in risk_cols_exist:
            print("Premium Analysis by Hazard Code:")
            display(df.filter(F.col("haz_cd_leg").isNotNull()) \
              .groupBy("haz_cd_leg") \
              .agg(
                  F.count("rec_id").alias("policy_count"),
                  F.sum("tot_wprm_amt").alias("total_premium"),
                  F.avg("tot_wprm_amt").alias("avg_premium"),
                  F.avg("bas_rt").alias("avg_base_rate")
              ) \
              .orderBy(F.desc("policy_count")) \
              .limit(10))
        
        # Analyze by base rate correlation with premium
        if "bas_rt" in risk_cols_exist:
            # Create rate bands for analysis
            rate_analysis = df.filter(F.col("bas_rt").isNotNull())
            
            # Create rate bands based on percentiles
            rate_percentiles = rate_analysis.selectExpr(
                "percentile(bas_rt, array(0.2, 0.4, 0.6, 0.8)) as percentiles"
            ).collect()[0]["percentiles"]
            
            rate_analysis = rate_analysis.withColumn(
                "rate_band",
                F.when(F.col("bas_rt") <= rate_percentiles[0], "Very Low")
                 .when(F.col("bas_rt") <= rate_percentiles[1], "Low")
                 .when(F.col("bas_rt") <= rate_percentiles[2], "Medium")
                 .when(F.col("bas_rt") <= rate_percentiles[3], "High")
                 .otherwise("Very High")
            )
            
            print("Premium Analysis by Base Rate Band:")
            display(rate_analysis.groupBy("rate_band") \
                        .agg(
                            F.count("rec_id").alias("policy_count"),
                            F.avg("bas_rt").alias("avg_base_rate"),
                            F.avg("tot_wprm_amt").alias("avg_premium"),
                            F.sum("tot_wprm_amt").alias("total_premium")
                        ) \
                        .orderBy(
                            F.when(F.col("rate_band") == "Very Low", 1)
                             .when(F.col("rate_band") == "Low", 2)
                             .when(F.col("rate_band") == "Medium", 3)
                             .when(F.col("rate_band") == "High", 4)
                             .when(F.col("rate_band") == "Very High", 5)
                        ))
        
        # Product code analysis
        if "rtmk_prdct_cd_leg" in risk_cols_exist:
            print("Premium Analysis by Product Code:")
            display(df.groupBy("rtmk_prdct_cd_leg") \
              .agg(
                  F.count("rec_id").alias("policy_count"),
                  F.sum("tot_wprm_amt").alias("total_premium"),
                  F.avg("tot_wprm_amt").alias("avg_premium")
              ) \
              .orderBy(F.desc("policy_count")) \
              .limit(10))
        
        # Premium type analysis
        if "prem_typ_cd" in risk_cols_exist:
            print("Premium Analysis by Premium Type:")
            display(df.groupBy("prem_typ_cd") \
              .agg(
                  F.count("rec_id").alias("policy_count"),
                  F.sum("tot_wprm_amt").alias("total_premium"),
                  F.avg("tot_wprm_amt").alias("avg_premium")
              ) \
              .orderBy(F.desc("policy_count")))
        
        return df.select("rec_id", *risk_cols_exist, "tot_wprm_amt")

# 16. NEW: Policy Renewal Analysis
def renewal_analysis(df):
    """Analyze policy renewals and retention"""
    
    # Check if renewal code column exists
    if "new_ren_cd" in df.columns:
        print("Policy Distribution by Renewal Status:")
        display(df.groupBy("new_ren_cd") \
          .agg(
              F.count("rec_id").alias("policy_count"),
              F.sum("tot_wprm_amt").alias("total_premium"),
              F.avg("tot_wprm_amt").alias("avg_premium")
          ) \
          .orderBy(F.desc("policy_count")))
        
        # Renewal analysis by state
        print("Renewal Rate by State:")
        state_renewal = df.groupBy("acct_st_cd", "new_ren_cd") \
                         .count() \
                         .groupBy("acct_st_cd") \
                         .pivot("new_ren_cd") \
                         .sum("count") \
                         .fillna(0)
        
        # Add renewal rate column - calculated as renewed/(renewed+new)
        renewal_cols = [col for col in state_renewal.columns if col != "acct_st_cd"]
        if len(renewal_cols) > 1:
            # Assuming 'R' is the code for renewals and 'N' for new
            if 'R' in renewal_cols and 'N' in renewal_cols:
                state_renewal = state_renewal.withColumn(
                    "renewal_rate",
                    F.col("R") / (F.col("R") + F.col("N")) * 100
                )
                
                display(state_renewal.orderBy(F.desc("renewal_rate")).limit(10))
        
        # Premium analysis by renewal status and policy form
        print("Premium by Renewal Status and Policy Form:")
        display(df.groupBy("new_ren_cd", "pol_fm_cd") \
          .agg(
              F.count("rec_id").alias("policy_count"),
              F.sum("tot_wprm_amt").alias("total_premium"),
              F.avg("tot_wprm_amt").alias("avg_premium")
          ) \
          .orderBy("new_ren_cd", F.desc("policy_count")) \
          .limit(15))
        
        return state_renewal

# 17. NEW: Premium vs Earned Premium Analysis
def premium_earned_analysis(df):
    """Analyze written vs earned premium patterns"""
    
    # Check if required columns exist
    premium_cols = ["tot_wprm_amt", "cy_yr1_ep_amt", "cy_yr2_ep_amt"]
    premium_cols_exist = [col for col in premium_cols if col in df.columns]
    
    if len(premium_cols_exist) >= 2 and "tot_wprm_amt" in premium_cols_exist:
        # Calculate earned-to-written premium ratio
        if "cy_yr1_ep_amt" in premium_cols_exist:
            earned_written_ratio = df.withColumn(
                "ep_wp_ratio",
                F.when(F.col("tot_wprm_amt") > 0,
                       F.col("cy_yr1_ep_amt") / F.col("tot_wprm_amt")
                ).otherwise(None)
            )
            
            print("Earned to Written Premium Ratio Statistics:")
            display(earned_written_ratio.select(
                F.avg("ep_wp_ratio").alias("avg_ratio"),
                F.min("ep_wp_ratio").alias("min_ratio"),
                F.max("ep_wp_ratio").alias("max_ratio"),
                F.stddev("ep_wp_ratio").alias("stddev_ratio")
            ))
            
            # Earned premium ratio by state
            print("Earned to Written Premium Ratio by State:")
            display(earned_written_ratio.groupBy("acct_st_cd") \
                               .agg(
                                   F.avg("ep_wp_ratio").alias("avg_ratio"),
                                   F.sum("cy_yr1_ep_amt").alias("total_earned_premium"),
                                   F.sum("tot_wprm_amt").alias("total_written_premium"),
                                   F.count("rec_id").alias("policy_count")
                               ) \
                               .orderBy(F.desc("policy_count")) \
                               .limit(10))
            
            # Earned premium ratio by policy form
            print("Earned to Written Premium Ratio by Policy Form:")
            display(earned_written_ratio.groupBy("pol_fm_cd") \
                               .agg(
                                   F.avg("ep_wp_ratio").alias("avg_ratio"),
                                   F.sum("cy_yr1_ep_amt").alias("total_earned_premium"),
                                   F.sum("tot_wprm_amt").alias("total_written_premium"),
                                   F.count("rec_id").alias("policy_count")
                               ) \
                               .orderBy(F.desc("policy_count")) \
                               .limit(10))
            
            return earned_written_ratio

# Main execution function
def main():
    # Load the data
    df = spark.table("")
    
    # Run selected analyses
    explore_data(df)
    df_with_date = time_analysis(df)
    premium_by_form = premium_analysis(df)
    state_premium = geographic_analysis(df)
    state_ratios = premium_exposure_analysis(df)
    quarterly_premiums = quarterly_trends(df)
    industry_summary = industry_analysis(df)
    decile_summary = premium_distribution(df)
    state_commission = commission_analysis(df)
    growth_summary = yoy_growth_analysis(df)
    location_risk = location_risk_analysis(df)
    coverage_summary = coverage_analysis(df)
    
    # New analyses
    quarterly_patterns = quarterly_premium_patterns(df)
    vehicle_data = vehicle_analysis(df)
    risk_data = risk_factor_analysis(df)
    renewal_data = renewal_analysis(df)
    earned_premium_data = premium_earned_analysis(df)
    
    # Save outputs to tables if needed
    # state_premium.write.mode("overwrite").saveAsTable("insurance_analysis.state_premium_summary")
    
    print("Analysis completed successfully!")
    
    # Return dictionary of results for further processing if needed
    return {
        "df_with_date": df_with_date,
        "premium_by_form": premium_by_form,
        "state_premium": state_premium,
        "state_ratios": state_ratios,
        "quarterly_premiums": quarterly_premiums,
        "industry_summary": industry_summary,
        "decile_summary": decile_summary,
        "state_commission": state_commission,
        "growth_summary": growth_summary,
        "location_risk": location_risk,
        "coverage_summary": coverage_summary,
        "quarterly_patterns": quarterly_patterns,
        "vehicle_data": vehicle_data,
        "risk_data": risk_data,
        "renewal_data": renewal_data,
        "earned_premium_data": earned_premium_data
    }

if __name__ == "__main__":
    main() 
