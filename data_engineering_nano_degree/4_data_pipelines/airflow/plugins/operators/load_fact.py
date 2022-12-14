from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
#from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self
                 , table
                 , redshift_conn_id='redshift'
                 , sql=''
                 , *args
                 , **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql

    def execute(self, context):
        #aws_hook = AwsHook("aws_credentials")
        #credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook("redshift")
        
        sql = """
        INSERT INTO {table}
        {sql}
        """.format(table=self.table, sql=self.sql)
        
        redshift_hook.run(sql)
