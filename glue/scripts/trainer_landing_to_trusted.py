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

# Script generated for node Step_Trainer_Landing
Step_Trainer_Landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://data-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Step_Trainer_Landing_node1",
)

# Script generated for node Customer_Curated
Customer_Curated_node1681553136144 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://data-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Curated_node1681553136144",
)

# Script generated for node Renamed_Col
Renamed_Col_node1681553285758 = ApplyMapping.apply(
    frame=Customer_Curated_node1681553136144,
    mappings=[("serialNumber", "string", "cc_serialNumber", "string")],
    transformation_ctx="Renamed_Col_node1681553285758",
)

# Script generated for node Inner_Join
Inner_Join_node2 = Join.apply(
    frame1=Step_Trainer_Landing_node1,
    frame2=Renamed_Col_node1681553285758,
    keys1=["serialNumber"],
    keys2=["cc_serialNumber"],
    transformation_ctx="Inner_Join_node2",
)

# Script generated for node Drop_Fields
Drop_Fields_node1681553350420 = DropFields.apply(
    frame=Inner_Join_node2,
    paths=["cc_serialNumber"],
    transformation_ctx="Drop_Fields_node1681553350420",
)

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Drop_Fields_node1681553350420,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://data-lake-house/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Step_Trainer_Trusted_node3",
)

job.commit()
