from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    sql_template_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        TIMEFORMAT AS 'epochmillisecs'
        FORMAT AS JSON 'auto' -- or use log_json_path.json in bucket root
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        # AWS connection
        self.log.info("Connecting to S3")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        # use temp path to test
        s3_path = "s3://{}/{}/".format(
            self.s3_bucket,
            self.s3_key
        )

        # Redshift Conn
        self.log.info("Connecting to Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear existing data from Redshift staging table
        clear_msg = "Deleting old records in staging table {} from redshift"
        self.log.info(clear_msg.format(self.table))
        redshift.run("DELETE FROM {}".format(self.table))

        # Copy data from S3 to Redshift
        self.log.info("Copying specified data to staging table")
        copy_data_sql = StageToRedshiftOperator.sql_template_json.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key
        )

        redshift.run(copy_data_sql)
        self.log.info("Copying to staging table complete")
