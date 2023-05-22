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

# Script generated for node Accelerometer_Landing
Accelerometer_Landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://data-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Landing_node1",
)

# Script generated for node Customer_Trusted
Customer_Trusted_node1681524865549 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://data-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Trusted_node1681524865549",
)

# Script generated for node Inner_Join
Inner_Join_node2 = Join.apply(
    frame1=Accelerometer_Landing_node1,
    frame2=Customer_Trusted_node1681524865549,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Inner_Join_node2",
)

# Script generated for node Drop_Field
Drop_Field_node1681525371954 = DropFields.apply(
    frame=Inner_Join_node2,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="Drop_Field_node1681525371954",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Drop_Field_node1681525371954,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://data-lake-house/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Accelerometer_Trusted_node3",
)

job.commit()
