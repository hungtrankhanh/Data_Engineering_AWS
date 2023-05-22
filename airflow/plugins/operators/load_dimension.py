from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import sql_queries


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    insert_sql = """INSERT INTO {} {} {}"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dimension_table="",
                 columns="",
                 query="",
                 clear_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dimension_table = dimension_table
        self.columns = columns
        self.query = query
        self.clear_table = clear_table

    def execute(self, context):
        self.log.info(f'LoadDimensionOperator table {self.dimension_table}')
        aws_redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.clear_table:
            self.log.info("Clearing data from Redshift dimension table")
            aws_redshift_hook.run(f"DELETE FROM {self.dimension_table}")

        self.log.info("Loading data from staging tables into dimension table")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.dimension_table,
            self.columns,
            self.query
        )
        aws_redshift_hook.run(formatted_sql)