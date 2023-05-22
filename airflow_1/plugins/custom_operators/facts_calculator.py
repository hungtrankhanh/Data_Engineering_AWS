import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FactsCalculatorOperator(BaseOperator):
    facts_sql_template = """
    DROP TABLE IF EXISTS {destination_table};
    CREATE TABLE {destination_table} AS
    SELECT
        {groupby_column},
        MAX({fact_column}) AS max_{fact_column},
        MIN({fact_column}) AS min_{fact_column},
        AVG({fact_column}) AS average_{fact_column}
    FROM {origin_table}
    GROUP BY {groupby_column};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 origin_table="",
                 destination_table="",
                 fact_column="",
                 groupby_column="",
                 *args, **kwargs):

        super(FactsCalculatorOperator, self).__init__(*args, **kwargs)
        #
        # TODO: Set attributes from __init__ instantiation arguments
        #
        self.redshift_conn_id = redshift_conn_id
        self.origin_table = origin_table
        self.destination_table = destination_table
        self.fact_column = fact_column
        self.groupby_column = groupby_column

    def execute(self, context):
        #
        # TODO: Fetch the redshift hook
        #
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f'Create {self.destination_table} from {self.origin_table}')
        redshift_hook.run(FactsCalculatorOperator.facts_sql_template.format(origin_table = self.origin_table, destination_table= self.destination_table, fact_column= self.fact_column, groupby_column = self.groupby_column))

        #
        # TODO: Format the `facts_sql_template` and run the query against redshift
        #
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.destination_table}")
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.destination_table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.destination_table} contained 0 rows")
        logging.info(f"Data quality on table {self.destination_table} check passed with {records[0][0]} records")


