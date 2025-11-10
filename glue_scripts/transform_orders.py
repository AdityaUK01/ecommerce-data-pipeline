# transform_orders.py
# Glue PySpark script (Glue 3.0 / Spark 3.x compatible)
# Arguments:
#  --S3_INPUT_PREFIX  (e.g. s3://.../raw/orders/date=2025-11-01/)
#  --S3_OUTPUT_PREFIX (e.g. s3://.../clean/orders/)
#  --PROCESS_DATE     (YYYY-MM-DD)
#  --TEMP_DIR         (Glue temp dir e.g. s3://.../glue-temp/)

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import sys
import pyspark.sql.functions as F
from pyspark.sql.types import DateType

args = getResolvedOptions(sys.argv,
                          ['JOB_NAME','S3_INPUT_PREFIX','S3_OUTPUT_PREFIX','PROCESS_DATE','TEMP_DIR'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

input_prefix = args['S3_INPUT_PREFIX'].rstrip('/') + '/'
output_prefix = args['S3_OUTPUT_PREFIX'].rstrip('/') + '/'
process_date = args['PROCESS_DATE']
temp_dir = args['TEMP_DIR']

# Read all CSVs from input prefix (handles .csv and .csv.gz)
df = spark.read.option("header", True).option("inferSchema", True).csv(input_prefix + "*")

if df.rdd.isEmpty():
    print(f"[WARN] No files found at {input_prefix}. Exiting.")
    job.commit()
    sys.exit(0)

# Standardize column names
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip())

# Normalize date column
date_cols = [c for c in df.columns if c.lower().strip() in ("order_date","date")]
if date_cols:
    date_col = date_cols[0]
    df = df.withColumn("order_date_parsed", F.to_date(F.col(date_col).cast("string"), "yyyy-MM-dd"))
else:
    # fallback: try to parse an existing column named 'Order_Date' or create from PROCESS_DATE
    df = df.withColumn("order_date_parsed", F.lit(process_date).cast(DateType()))

# Drop rows with null order_date_parsed
df = df.filter(F.col("order_date_parsed").isNotNull())

# Clean and cast numeric columns if exist
numeric_cols = ['Sales','Quantity','Discount','Profit','Shipping_Cost']
for nc in numeric_cols:
    if nc in df.columns:
        df = df.withColumn(nc, F.when(F.col(nc)=='' , None).otherwise(F.col(nc).cast("double")))

# Trim string columns
string_cols = [c for c, t in df.dtypes if t == 'string']
for scn in string_cols:
    df = df.withColumn(scn, F.trim(F.col(scn)))

# Create canonical columns needed for downstream
# Example canonical: order_id, customer_id, product_id, price, quantity, order_date
if 'Order_Id' in df.columns or 'order_id' in df.columns:
    if 'Order_Id' in df.columns:
        df = df.withColumnRenamed('Order_Id','order_id')
    else:
        df = df.withColumnRenamed('order_id','order_id')
else:
    # synthesize order id if missing
    df = df.withColumn("order_id", F.concat_ws("_", F.col("Customer_Id"), F.col("order_date_parsed"), F.monotonically_increasing_id()))

if 'Customer_Id' in df.columns:
    df = df.withColumnRenamed('Customer_Id','customer_id')
elif 'CustomerId' in df.columns:
    df = df.withColumnRenamed('CustomerId','customer_id')

if 'Product' in df.columns:
    df = df.withColumnRenamed('Product','product_name')

# Partition by order_date_parsed (write path uses dt=YYYY-MM-DD)
df = df.withColumn("dt", F.date_format(F.col("order_date_parsed"), "yyyy-MM-dd"))

# Write to parquet, partitioned by dt
out_path = output_prefix.rstrip('/') + "/"
(df.repartition(200)  # adjust repartition to your dataset size
   .write
   .mode("append")
   .partitionBy("dt")
   .parquet(out_path)
)

print(f"[INFO] Wrote cleaned parquet to: {out_path} partitioned by dt={process_date}")

job.commit()

