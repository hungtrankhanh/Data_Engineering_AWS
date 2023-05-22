from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        copy {}
        from '{}'
        access_key_id '{}'
        secret_access_key '{}'
        format as json '{}' compupdate off region '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 staging_table="",
                 s3_bucket_path="",
                 json_format="",
                 region="",
                 clear_table=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.staging_table = staging_table
        self.s3_bucket_path = s3_bucket_path
        self.json_format = json_format
        self.region = region
        self.clear_table = clear_table

    def execute(self, context):
        self.log.info(f'StageToRedshiftOperator : table {self.staging_table}')
        aws_redshift_hook = PostgresHook(self.redshift_conn_id)
        aws_credential_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_credential_hook.get_credentials()

        if self.clear_table:
            self.log.info("Clearing data from staging Redshift table")
            aws_redshift_hook.run(f"DELETE FROM {self.staging_table}")

        self.log.info("Copying data from S3 to Redshift")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.staging_table,
            self.s3_bucket_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_format,
            self.region
        )
        aws_redshift_hook.run(formatted_sql)







