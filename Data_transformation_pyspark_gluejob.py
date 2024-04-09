import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import *
import pandas as pd
from datetime import datetime,timedelta
from pyspark.sql.functions import col, to_date,when


args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#-------------------------Reading file from S3 which comes from RIC team--------------------------------------------------
#Target bucket for unloading data
BUCKET_NAME = "kafka-inital-test-bucket"
PREFIX = "RIC_RTB_Automation/To_Edap"

client = boto3.client('s3')

#fetch current date
date_time = datetime.now().strftime("%Y-%m-%d")

#getting S3 response
response = client.list_objects(
    Bucket=BUCKET_NAME,
    Prefix=PREFIX,
)

#returns the name of the output file
name = response["Contents"][-1]["Key"]
lastpick=name.split(sep="/")[-1]  #picked the last value after /

#reading the file from the S3 path
df = spark.read.option("header","true").option("delimiter", ",").option("ignoreLeadingWhiteSpace", "true").option("ignoreTrailingWhiteSpace", "true").csv("s3://kafka-inital-test-bucket/RIC_RTB_Automation/To_Edap/"+lastpick)

# print('input file is')  #uncomment to check schema of the file
# df.printSchema()

df.createTempView('File_received_RIC')

Rtb_result = spark.sql("""
    SELECT 
        MID as RTB_MID,
        `Date of Request` as Date_of_Request_RTB,
        'RTB file' as from_file,
        `RIC Analyst` as RIC_Analyst,
        MCC as RTB_MCC, 
        Amex as RTB_Amex,
        International as RTB_International, 
        `Cards P1` as Cards_P1_RTB, 
        `Cards P2` as Cards_P2_RTB, 
        `Cards P3`as Cards_P3_RTB
    FROM File_received_RIC
""")

#Rtb_result.createTempView("Rtb_result_query")
total_row_count_Rtb = Rtb_result.count()

Rtb_result.filter(Rtb_result["Date_of_Request_RTB"].isNotNull()).show()

# print(f'total count of Rtb file is ',total_row_count_Rtb) #uncomment to find the total rows in Rtb_result data

#-------------Redshift query for merchant data that is saved in our end----------------------------------------------------------

Merchant_data =spark.read.format("jdbc")\
.option("url", "")\
.option("dbtable",f"""(

select distinct
    asu.mid as mid_from_system,
    asu.merchant_id as merchant_id_from_system,
    asu.submerchant_id as submerchant_id_from_system,
    asu.emi_merchant_name as merchant_name_from_system,
    asu.merchant_website as merchant_website_from_system,
    coalesce(pbd3.email, m.email) as email_from_system,
    coalesce(pbd3.phone, m.mobile_number) as mobile_number_from_system,
    coalesce(pbd3.address, m.address) as address_from_system,
    coalesce(pbd3.pincode, m.pincode) as pincode,
    coalesce(pbd3.city, m.city) as city_from_system,
    coalesce(pbd3.state, m.state) as state_from_system,
    asu.business_domain as business_domain_from_system,
    coalesce(pbd2.pan_number, b2.pan_id) as pan_id_from_system,
    coalesce(pbd2.pan_owner_name, b2.pan_owner_name) as pan_owner_name_from_system,
    coalesce(pbd2.business_name, b2.business_name) as business_name_from_system,
    coalesce(pbd2.gstin, b2.gstin) as gstin_from_system,
    case when b2.isorganization = 1 then 'Proprietorship Firm'
     when b2.isorganization = 2 then 'Private Limited or Partnership Firm'
     when b2.isorganization = 3 then 'NGO / Company u/s 25'
     when b2.isorganization = 4 then 'Trust/Education'
     when b2.isorganization = 5 then 'HUF (Hindu Undivided Family)'
     when b2.isorganization = 6 then 'College and Universities'
     when b2.isorganization = 7 then 'Limited Liability Partnership'
     when b2.isorganization = 8 then 'Private Limited'
     when b2.isorganization = 9 then 'Partnership Firm'
     when b2.isorganization = 0 then 'Individual/Freelancer' 
     when b2.isorganization = 10 then 'Sole Proprietor'
     when b2.isorganization = 11 then 'Private Ltd/Public Ltd/OPC'
     when b2.isorganization = 12 then 'Trust/NGO - Donations'
     when b2.isorganization = 13 then 'Government Entity'
     when b2.isorganization = 14 then 'Co-operative Credit Society/JV/AOP'
     else Null
     end as entity_type_from_system,
    coalesce(pbd.approved_bank_name, b.approved_bank_name) as approved_bank_name_from_system,
    coalesce(pbd.approved_account_number,b.approved_account_number) as approved_account_number_from_system,
    asu.rm_name as rm_name_from_system,
    asu.rm_region as rm_region_from_system,
    asu.signup_date as signup_date_from_system,
    asu.gone_live_date_overall as gone_live_date_from_system,
    asu.merchant_status_pg as merchant_status_pg_from_system,
    asu.merchant_status_wire as merchant_status_wire_from_system,
    min(case when verification_type= 'AUSPAN' and verification_status= 'true' and pbd2.authorized_signatory_pan_number=mv.verification_value then json_extract_path_text(merchant_documents, 'name') else null end) as AUS_name_from_system,
    min(case when verification_type= 'PAN' and verification_status= 'true' and pbd2.pan_number=mv.verification_value then json_extract_path_text(merchant_documents, 'name') else null end) as PAN_name_from_system
from prod_easebuzz_db.eb_merchant_info m
    inner join prod_staging.all_signed_up asu on asu.mid = m.id::varchar 
    left outer join prod_easebuzz_db.eb_profile_bankdetails b on m.id = b.merchant_id
    left outer join prod_easebuzz_db.eb_profile_businessinfo b2 on m.id = b2.merchant_id
    left outer join prod_kyc_db.merchant_info emi on emi.easebuzz_id::int=m.id and emi.application = 'easebuzz'
    left outer join prod_kyc_db.profile_bank_details pbd on pbd.merchant_id_id = emi.id
    left outer join prod_external_schema.ckyc_profile_business_details pbd2 on pbd2.merchant_id_id = emi.id
    left outer join prod_external_schema.ckyc_merchant_verification mv on emi.id = mv.merchant_id_id 
    left outer join prod_external_schema.ckyc_profile_basic_details pbd3 on pbd3.merchant_id_id= emi.id
group by 
    asu.mid,
    asu.merchant_id,
    asu.submerchant_id,
    asu.emi_merchant_name,
    asu.merchant_website,
    coalesce(pbd3.email, m.email),
    coalesce(pbd3.phone, m.mobile_number),
    coalesce(pbd3.address, m.address),
    coalesce(pbd3.pincode, m.pincode),
    coalesce(pbd3.city, m.city),
    coalesce(pbd3.state, m.state),
    asu.business_domain,
    coalesce(pbd2.pan_number, b2.pan_id),
    coalesce(pbd2.pan_owner_name, b2.pan_owner_name),
    coalesce(pbd2.business_name, b2.business_name),
    coalesce(pbd2.gstin, b2.gstin),
    case when b2.isorganization = 1 then 'Proprietorship Firm'
     when b2.isorganization = 2 then 'Private Limited or Partnership Firm'
     when b2.isorganization = 3 then 'NGO / Company u/s 25'
     when b2.isorganization = 4 then 'Trust/Education'
     when b2.isorganization = 5 then 'HUF (Hindu Undivided Family)'
     when b2.isorganization = 6 then 'College and Universities'
     when b2.isorganization = 7 then 'Limited Liability Partnership'
     when b2.isorganization = 8 then 'Private Limited'
     when b2.isorganization = 9 then 'Partnership Firm'
     when b2.isorganization = 0 then 'Individual/Freelancer' 
     when b2.isorganization = 10 then 'Sole Proprietor'
     when b2.isorganization = 11 then 'Private Ltd/Public Ltd/OPC'
     when b2.isorganization = 12 then 'Trust/NGO - Donations'
     when b2.isorganization = 13 then 'Government Entity'
     when b2.isorganization = 14 then 'Co-operative Credit Society/JV/AOP'
     else Null
     end,
    coalesce(pbd.approved_bank_name, b.approved_bank_name),
    coalesce(pbd.approved_account_number,b.approved_account_number),
    coalesce(pbd2.authorized_signatory_name,b2.authorized_signatory_name),
    asu.rm_name,
    asu.rm_region,
    asu.signup_date,
    asu.gone_live_date_overall,
    asu.merchant_status_pg,
    asu.merchant_status_wire
union 
select distinct
    asu.mid as mid,
    asu.merchant_id as merchant_id,
    asu.submerchant_id as submerchant_id,
    asu.emi_merchant_name as merchant_name,
    asu.merchant_website,
    coalesce(pbd3.email, m.email) as email,
    coalesce(pbd3.phone, m.mobile_number) as mobile_number,
    coalesce(pbd3.address, m.address) as address,
    coalesce(pbd3.pincode, m.pincode) as pincode,
    coalesce(pbd3.city, m.city) as city,
    coalesce(pbd3.state, m.state) as state,
    asu.business_domain,
    coalesce(pbd2.pan_number, b2.pan_id) as pan_id,
    coalesce(pbd2.pan_owner_name, b2.pan_owner_name) as pan_owner_name,
    coalesce(pbd2.business_name, b2.business_name) as business_name,
    coalesce(pbd2.gstin, b2.gstin) as gstin,
    case when b2.isorganization = 1 then 'Proprietorship Firm'
     when b2.isorganization = 2 then 'Private Limited or Partnership Firm'
     when b2.isorganization = 3 then 'NGO / Company u/s 25'
     when b2.isorganization = 4 then 'Trust/Education'
     when b2.isorganization = 5 then 'HUF (Hindu Undivided Family)'
     when b2.isorganization = 6 then 'College and Universities'
     when b2.isorganization = 7 then 'Limited Liability Partnership'
     when b2.isorganization = 8 then 'Private Limited'
     when b2.isorganization = 9 then 'Partnership Firm'
     when b2.isorganization = 0 then 'Individual/Freelancer' 
     when b2.isorganization = 10 then 'Sole Proprietor'
     when b2.isorganization = 11 then 'Private Ltd/Public Ltd/OPC'
     when b2.isorganization = 12 then 'Trust/NGO - Donations'
     when b2.isorganization = 13 then 'Government Entity'
     when b2.isorganization = 14 then 'Co-operative Credit Society/JV/AOP'
     else Null
     end as entity_type,
    coalesce(pbd.approved_bank_name, b.approved_bank_name) as approved_bank_name,
    coalesce(pbd.approved_account_number,b.approved_account_number) as approved_account_number,
    asu.rm_name,
    asu.rm_region,
    asu.signup_date,
    asu.gone_live_date_overall as gone_live_date,
    asu.merchant_status_pg,
    asu.merchant_status_wire,
    min(case when verification_type= 'AUSPAN' and verification_status= 'true' and pbd2.authorized_signatory_pan_number=mv.verification_value then json_extract_path_text(merchant_documents, 'name') else null end) as AUS_name,
    min(case when verification_type= 'PAN' and verification_status= 'true' and pbd2.pan_number=mv.verification_value then json_extract_path_text(merchant_documents, 'name') else null end) as PAN_name
from prod_easebuzz_db.eb_merchant_info m
    inner join prod_staging.all_signed_up asu on asu.merchant_id = m.id::varchar and asu.submerchant_id is not null
    left outer join prod_easebuzz_db.eb_profile_bankdetails b on m.id = b.merchant_id
    left outer join prod_easebuzz_db.eb_profile_businessinfo b2 on m.id = b2.merchant_id
    left outer join prod_kyc_db.merchant_info emi on emi.eb_submerchant_id=asu.submerchant_id and emi.application = 'pg'
    left outer join prod_kyc_db.profile_bank_details pbd on pbd.merchant_id_id = emi.id
    left outer join prod_external_schema.ckyc_profile_business_details pbd2 on pbd2.merchant_id_id = emi.id
    left outer join prod_external_schema.ckyc_merchant_verification mv on emi.id = mv.merchant_id_id
    left outer join prod_external_schema.ckyc_profile_basic_details pbd3 on pbd3.merchant_id_id= emi.id
group by 
    asu.mid,
    asu.merchant_id,
    asu.submerchant_id,
    asu.emi_merchant_name,
    asu.merchant_website,
    coalesce(pbd3.email, m.email),
    coalesce(pbd3.phone, m.mobile_number),
    coalesce(pbd3.address, m.address),
    coalesce(pbd3.pincode, m.pincode),
    coalesce(pbd3.city, m.city),
    coalesce(pbd3.state, m.state),
    asu.business_domain,
    coalesce(pbd2.pan_number, b2.pan_id),
    coalesce(pbd2.pan_owner_name, b2.pan_owner_name),
    coalesce(pbd2.business_name, b2.business_name),
    coalesce(pbd2.gstin, b2.gstin),
    case when b2.isorganization = 1 then 'Proprietorship Firm'
     when b2.isorganization = 2 then 'Private Limited or Partnership Firm'
     when b2.isorganization = 3 then 'NGO / Company u/s 25'
     when b2.isorganization = 4 then 'Trust/Education'
     when b2.isorganization = 5 then 'HUF (Hindu Undivided Family)'
     when b2.isorganization = 6 then 'College and Universities'
     when b2.isorganization = 7 then 'Limited Liability Partnership'
     when b2.isorganization = 8 then 'Private Limited'
     when b2.isorganization = 9 then 'Partnership Firm'
     when b2.isorganization = 0 then 'Individual/Freelancer' 
     when b2.isorganization = 10 then 'Sole Proprietor'
     when b2.isorganization = 11 then 'Private Ltd/Public Ltd/OPC'
     when b2.isorganization = 12 then 'Trust/NGO - Donations'
     when b2.isorganization = 13 then 'Government Entity'
     when b2.isorganization = 14 then 'Co-operative Credit Society/JV/AOP'
     else Null
     end,
    coalesce(pbd.approved_bank_name, b.approved_bank_name),
    coalesce(pbd.approved_account_number,b.approved_account_number),
    coalesce(pbd2.authorized_signatory_name,b2.authorized_signatory_name),
    asu.rm_name,
    asu.rm_region,
    asu.signup_date,
    asu.gone_live_date_overall,
    asu.merchant_status_pg,
    asu.merchant_status_wire

 )as result_table""")\
.option("user", "")\
.option("password", "")\
.load().coalesce(1)
    
Merchant_data.createOrReplaceTempView("redshift_merchant_query")
    
Merchant_data_result = spark.sql("""
SELECT * 
FROM redshift_merchant_query
"""
    )

total_row_count_merchant = Merchant_data_result.count() 
# print(total_row_count_merchant,'total_row_count of merchant dataset is') #uncomment to find the total rows in merchant data
    
#--------------------------------Salesforce data----------------------------------------------------------

Salesforce_data =spark.read.format("jdbc")\
.option("url", "")\
.option("dbtable",f"""(
select 
distinct 
mid as mid_salesforce,
mcc as mcc_salesforce,
pg_sub_merchant_id as pg_sub_merchant_id_salesforce,
banking_alliance_lead_source as banking_alliance_lead_source_salesforce,
referral_partner_mid as referral_partner_mid_salesforce,
underwriting_approval_given_timestamp as underwriting_approval_given_timestamp_salesforce
from 
salesforce_prod.salesforce_opportunity_new
 )as result_table""")\
.option("user", "")\
.option("password", "")\
.load().coalesce(1)
    
Salesforce_data.createOrReplaceTempView("Redshift_Salesforce_data_query")
    
Salesforce_data_result = spark.sql("""
SELECT 
*
FROM Redshift_Salesforce_data_query 
"""
    )

total_row_count_salesforce = Salesforce_data_result.count() 
# print(total_row_count_salesforce,'total row count of salesforce dataset is') #uncomment to find the total rows in merchant data

if total_row_count_Rtb > 0 and total_row_count_merchant> 0 and total_row_count_salesforce> 0:

    # Convert Date_of_Request_RTB to date type in Rtb_result
    # Rtb_result = Rtb_result.withColumn("Date_of_Request_RTB", to_date(col("Date_of_Request_RTB")))
    
    # Left join between Salesforce_data and Merchant_data
    #df1 = Salesforce_data.join(Merchant_data, Salesforce_data["mid_salesforce"] == Merchant_data["mid_from_system"], "left")
    df1 = Merchant_data.join(Salesforce_data,Merchant_data["merchant_id_from_system"]==Salesforce_data["mid_salesforce"],"left")

    # Left join between df1 and Rtb_result
    final_df_1 = df1.join(Rtb_result, df1["mid_from_system"] == Rtb_result["RTB_MID"], "left")
    
    final_df_1.filter(final_df_1["Date_of_Request_RTB"].isNotNull()).show()
    
    final_df_1 = final_df_1.withColumn("from_file", when(col("from_file") == "RTB file", "RTB file").otherwise("From System"))
    
    # Show the schema of the resulting DataFrame
    final_df_1.printSchema()
    
    # # Show the first few rows of the resulting DataFrame
    # final_df_1.show()
    
    # total_row_count_Finaldf1 = final_df_1.count() 
    # print(total_row_count_Finaldf1,'total row count of final dataset is')
    
    #df2 = Salesforce_data.join(Merchant_data, Salesforce_data["pg_sub_merchant_id_salesforce"] == Merchant_data["submerchant_id_from_system"], "inner")
    df2 = Merchant_data.join(Salesforce_data,Merchant_data["submerchant_id_from_system"]==Salesforce_data["pg_sub_merchant_id_salesforce"],"inner")
    final_df_2 = df2.join(Rtb_result,df2["pg_sub_merchant_id_salesforce"]==Rtb_result["RTB_MID"],"inner")
    
    final_df_2 = final_df_2.withColumn("from_file", when(col("from_file") == "RTB file", "RTB file").otherwise("From System"))
    
    # Show the schema of the resulting DataFrame
    final_df_2.printSchema()
    
    # # Show the first few rows of the resulting DataFrame
    # final_df_2.show()
    
    union_df = final_df_1.union(final_df_2).coalesce(1)
    
    union_df = union_df.select("mid_from_system",
    "merchant_id_from_system",
    "submerchant_id_from_system",
    "merchant_name_from_system",
    "merchant_website_from_system",
    "email_from_system",
    "mobile_number_from_system",
    "address_from_system",
    "pincode",
    "city_from_system",
    "state_from_system",
    "business_domain_from_system",
    "pan_id_from_system",
    "pan_owner_name_from_system",
    "business_name_from_system",
    "gstin_from_system",
    "entity_type_from_system",
    "approved_bank_name_from_system",
    "approved_account_number_from_system",
    "rm_name_from_system",
    "rm_region_from_system",
    "signup_date_from_system",
    "gone_live_date_from_system",
    "merchant_status_pg_from_system",
    "merchant_status_wire_from_system",
    "aus_name_from_system",
    "pan_name_from_system",
    "banking_alliance_lead_source_salesforce",
    "referral_partner_mid_salesforce",
    "underwriting_approval_given_timestamp_salesforce",
    "Date_of_Request_RTB",
    "from_file",
    "RIC_Analyst",
    "mcc_salesforce",
    "RTB_MCC",
    "RTB_Amex",
    "RTB_International",
    "Cards_P1_RTB",
    "Cards_P2_RTB",
    "Cards_P3_RTB")
    
    union_df = union_df.withColumn("from_file", when(col("from_file") == "RTB file", "RTB file").otherwise(col("from_file")))
    
    print(union_df.count())
    # print(union_df.printSchema())

    #Target bucket for unloading data
    BUCKET_NAME = "kafka-inital-test-bucket"
    PREFIX = "RIC_RTB_Automation/From_Edap"

#--------------------------------Exporting data------------------------------------

    try:
        #converting to dynamic frame
        DyF = DynamicFrame.fromDF(union_df, glueContext, "DyF")
        
        # num_rows = DyF.count()

        # # Print the number of rows
        # print("Number of rows:", num_rows)
        
        #Exporting data
        datasink2 = glueContext.write_dynamic_frame.from_options(frame = DyF, connection_type = "s3", connection_options = {"path": "s3://" + BUCKET_NAME + "/" + PREFIX}, format = "csv", transformation_ctx = "datasink2")
    
        #renaming the file using BOTO3
        client = boto3.client('s3')
    
        #getting S3 response
        response = client.list_objects(
            Bucket=BUCKET_NAME,
            Prefix=PREFIX,
        )
    
        #returns the name of the output file
        name = response["Contents"][-1]["Key"]
    
        #fetch current date
        now = datetime.now()
        date_time = now.strftime("%Y%m%d%H%M%S")
        folder_date = now.strftime("%Y-%m-%d")
    
        #creating a copy as a new file with custom name and current date timestamp
        s3 = boto3.resource('s3')
        s3.Object(BUCKET_NAME, "RIC_RTB_Automation/From_Edap/"+folder_date+"/Merged_Rtb_file_for_"+date_time+".csv").copy_from(CopySource=BUCKET_NAME+"/"+name)
        
        #deleting the previous exported data
        s3.Object(BUCKET_NAME, name).delete() 
        
        print("Object to be deleted:", name)
    
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
else:
    print('Data source error')

job.commit()