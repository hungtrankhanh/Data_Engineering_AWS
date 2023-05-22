import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer_Trusted
Customer_Trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://data-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Trusted_node1",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1681550237984 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://data-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Trusted_node1681550237984",
)

# Script generated for node Inner_Join
Inner_Join_node2 = Join.apply(
    frame1=Customer_Trusted_node1,
    frame2=Accelerometer_Trusted_node1681550237984,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Inner_Join_node2",
)

# Script generated for node Drop_Fields
Drop_Fields_node1681550430616 = DropFields.apply(
    frame=Inner_Join_node2,
    paths=["z", "timeStamp", "user", "x", "y", "email", "phone"],
    transformation_ctx="Drop_Fields_node1681550430616",
)

# Script generated for node Customer_Curated
Customer_Curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Drop_Fields_node1681550430616,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://data-lake-house/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="Customer_Curated_node3",
)

job.commit()
