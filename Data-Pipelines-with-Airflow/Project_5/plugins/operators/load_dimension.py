from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 aws_credentials_id="",
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 delete_load=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.aws_credentials_id= aws_credentials_id,
        self.redshift_conn_id = redshift_conn_id,
        self.sql_query = sql_query,
        self.table = table,
        self.delete_load = delete_load,

    def execute(self, context):
      redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

      if self.delete_load:
        self.log.info(f"Run delete statement on table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

      redshift.run(self.sql_query)
      self.log.info(f"Table {self.table} loaded.")
