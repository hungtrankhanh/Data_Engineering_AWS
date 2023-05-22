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

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node1681609872545 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://data-lake-house/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Step_Trainer_Trusted_node1681609872545",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://data-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Trusted_node1",
)

# Script generated for node Inner_Join
Inner_Join_node2 = Join.apply(
    frame1=Step_Trainer_Trusted_node1681609872545,
    frame2=Accelerometer_Trusted_node1,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Inner_Join_node2",
)

# Script generated for node Drop_Fields
Drop_Fields_node1681610063349 = DropFields.apply(
    frame=Inner_Join_node2,
    paths=["timeStamp", "user"],
    transformation_ctx="Drop_Fields_node1681610063349",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Drop_Fields_node1681610063349,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://data-lake-house/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
