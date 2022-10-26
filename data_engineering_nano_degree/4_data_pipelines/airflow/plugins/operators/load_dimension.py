from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self
                 , table
                 , redshift_conn_id='redshift_conn_id'
                 , sql=''
                 , mode='append'
                 , *args
                 , **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql
        self.mode=mode
        
        
    def execute(self, context):
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")
        
        if self.mode == 'truncate':
            sql = """DELETE FROM {table};""".format(table=self.table)
            redshift_hook.run(sql)

        sql = """INSERT INTO {table} {sql}""".format(table=self.table, sql=self.sql)
        redshift_hook.run(sql)
        
        
