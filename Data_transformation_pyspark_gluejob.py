import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date, when
from datetime import datetime

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# -------------------------Reading file from S3 which comes from RIC team--------------------------------------------------
BUCKET_NAME = "kafka-inital-test-bucket"
PREFIX = "RIC_RTB_Automation/To_Edap"

client = boto3.client('s3')

# Fetch current date
date_time = datetime.now().strftime("%Y-%m-%d")

# Getting S3 response
response = client.list_objects(
    Bucket=BUCKET_NAME,
    Prefix=PREFIX,
)

# Returns the name of the output file
name = response["Contents"][-1]["Key"]
lastpick = name.split(sep="/")[-1]

# Reading the file from the S3 path
df = spark.read.option("header", "true").option("delimiter", ",").option("ignoreLeadingWhiteSpace", "true").option("ignoreTrailingWhiteSpace", "true").csv(f"s3://{BUCKET_NAME}/RIC_RTB_Automation/To_Edap/{lastpick}")

# ------------------------------ Process the RTB Data -----------------------------------
Rtb_result = df.select(
    col("MID").alias("RTB_MID"),
    col("Date of Request").alias("Date_of_Request_RTB"),
    lit("RTB file").alias("from_file"),
    col("RIC Analyst").alias("RIC_Analyst"),
    col("MCC").alias("RTB_MCC"),
    col("Amex").alias("RTB_Amex"),
    col("International").alias("RTB_International"),
    col("Cards P1").alias("Cards_P1_RTB"),
    col("Cards P2").alias("Cards_P2_RTB"),
    col("Cards P3").alias("Cards_P3_RTB")
)

total_row_count_Rtb = Rtb_result.count()
Rtb_result.filter(Rtb_result["Date_of_Request_RTB"].isNotNull()).show()

# -------------------- Redshift query for merchant data that is saved in our end (via JDBC) ----------------------
Merchant_data = spark.read.format("jdbc") \
    .option("url", "") \
    .option("dbtable", """(select * from prod_easebuzz_db.eb_merchant_info) as result_table""") \
    .option("user", "") \
    .option("password", "") \
    .load().coalesce(1)

total_row_count_merchant = Merchant_data.count()

# -------------------------- Salesforce Data Query (via JDBC) ---------------------------
Salesforce_data = spark.read.format("jdbc") \
    .option("url", "") \
    .option("dbtable", """(select distinct mid, mcc, pg_sub_merchant_id, banking_alliance_lead_source, referral_partner_mid, underwriting_approval_given_timestamp from salesforce_prod.salesforce_opportunity_new) as result_table""") \
    .option("user", "") \
    .option("password", "") \
    .load().coalesce(1)

total_row_count_salesforce = Salesforce_data.count()

# --------------------------- Joining DataFrames and Processing --------------------------
if total_row_count_Rtb > 0 and total_row_count_merchant > 0 and total_row_count_salesforce > 0:
    
    # First join between Merchant_data and Salesforce_data
    df1 = Merchant_data.join(Salesforce_data, Merchant_data["merchant_id"] == Salesforce_data["mid"], "left")
    
    # Second join between df1 and Rtb_result
    final_df_1 = df1.join(Rtb_result, df1["mid"] == Rtb_result["RTB_MID"], "left")
    final_df_1 = final_df_1.withColumn("from_file", when(col("from_file") == "RTB file", "RTB file").otherwise("From System"))
    
    # Second join on submerchant_id field
    df2 = Merchant_data.join(Salesforce_data, Merchant_data["submerchant_id"] == Salesforce_data["pg_sub_merchant_id"], "inner")
    final_df_2 = df2.join(Rtb_result, df2["pg_sub_merchant_id"] == Rtb_result["RTB_MID"], "inner")
    final_df_2 = final_df_2.withColumn("from_file", when(col("from_file") == "RTB file", "RTB file").otherwise("From System"))
    
    # Union the two final DataFrames
    union_df = final_df_1.union(final_df_2).coalesce(1)
    
    # Selecting relevant columns from the union
    union_df = union_df.select(
        "mid",
        "merchant_id",
        "submerchant_id",
        "merchant_name",
        "merchant_website",
        "email",
        "mobile_number",
        "address",
        "pincode",
        "city",
        "state",
        "business_domain",
        "pan_id",
        "pan_owner_name",
        "business_name",
        "gstin",
        "entity_type",
        "approved_bank_name",
        "approved_account_number",
        "rm_name",
        "rm_region",
        "signup_date",
        "gone_live_date",
        "merchant_status_pg",
        "merchant_status_wire",
        "aus_name",
        "pan_name",
        "banking_alliance_lead_source",
        "referral_partner_mid",
        "underwriting_approval_given_timestamp",
        "Date_of_Request_RTB",
        "from_file",
        "RIC_Analyst",
        "mcc_salesforce",
        "RTB_MCC",
        "RTB_Amex",
        "RTB_International",
        "Cards_P1_RTB",
        "Cards_P2_RTB",
        "Cards_P3_RTB"
    )
    
    union_df = union_df.withColumn("from_file", when(col("from_file") == "RTB file", "RTB file").otherwise(col("from_file")))
    
    # Print the final row count
    print(union_df.count())
    
    # Exporting data to S3
    BUCKET_NAME = "kafka-inital-test-bucket"
    PREFIX = "RIC_RTB_Automation/From_Edap"

    try:
        DyF = DynamicFrame.fromDF(union_df, glueContext, "DyF")
        datasink2 = glueContext.write_dynamic_frame.from_options(
            frame=DyF, 
            connection_type="s3", 
            connection_options={"path": "s3://" + BUCKET_NAME + "/" + PREFIX}, 
            format="csv"
        )

        # Renaming the file using Boto3
        response = client.list_objects(Bucket=BUCKET_NAME, Prefix=PREFIX)
        name = response["Contents"][-1]["Key"]

        now = datetime.now()
        date_time = now.strftime("%Y%m%d%H%M%S")
        folder_date = now.strftime("%Y-%m-%d")

        s3 = boto3.resource('s3')
        s3.Object(BUCKET_NAME, f"RIC_RTB_Automation/From_Edap/{folder_date}/Merged_Rtb_file_for_{date_time}.csv").copy_from(CopySource=f"{BUCKET_NAME}/{name}")
        
        s3.Object(BUCKET_NAME, name).delete()

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
else:
    print('Data source error')

job.commit()
