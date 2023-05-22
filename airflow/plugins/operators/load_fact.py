from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'
    insert_sql = """INSERT INTO {} {} {}"""

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 fact_table="",
                 columns="",
                 query="",
                 clear_table=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.columns = columns
        self.query = query
        self.clear_table = clear_table

    def execute(self, context):
        self.log.info(f'LoadFactOperator table {self.fact_table}')
        aws_redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.clear_table:
            self.log.info("Clearing data from Redshift fact table")
            aws_redshift_hook.run(f"DELETE FROM {self.fact_table}")

        self.log.info("Loading data from staging tables into fact table")
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.fact_table,
            self.columns,
            self.query
        )
        aws_redshift_hook.run(formatted_sql)
