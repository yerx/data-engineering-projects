from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        redshift_conn_id='',
        table_name='',
        sql_statement='',
        mode='delete_load',
        *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_statement = sql_statement
        self.mode = mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == 'delete_load':
            # Clear old data from dim table
            self.log.info('refreshing data in table %s' % self.table_name)
            clear_statement = 'DELETE FROM %s' % self.table_name
            redshift.run(clear_statement)

            # Write new data into dim table
            self.log.info('Starting to load dim table %s' % self.table_name)
            sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.sql_statement)
            redshift.run(sql_statement)
            self.log.info('Dim table %s load finished' % self.table_name)

        elif self.mode == 'append':
            # Write new data into dim table
            self.log.info('Starting to load dim table %s' % self.table_name)
            sql_statement = 'INSERT INTO %s %s' % (self.table_name, self.sql_statement)
            redshift.run(sql_statement)
            self.log.info('Dim table %s load finished' % self.table_name)
        else:
            msg = 'invalid mode.  Mode must be one of "append" or "delete_load"'
            self.log.error(msg)
