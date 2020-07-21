from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 aws_credentials_id="",
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id,
        self.redshift_conn_id = redshift_conn_id,
        self.table=table,
        self.sql_query = sql_query,

    def execute(self, context):
      redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

      redshift.run(self.sql_query)
      self.log.info(f"Fact table {self.table} loaded")
