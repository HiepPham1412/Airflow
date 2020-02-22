from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_query = """
            COPY {table}
            FROM '{s3_path}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            format as {data_format} '{format_type}' 
            {copy_options};
        """

    @apply_defaults
    def __init__(self,
                 table,                 
                 s3_bucket,
                 s3_key,
                 data_format='json',
                 format_type='auto',
                 redshift_conn_id='redshift',
                 aws_conn_id='aws_credentials',
                 copy_options=tuple(),
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.data_format = data_format
        self.format_type = format_type
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        credentials = self.s3.get_credentials()
        copy_options = '\n\t\t\t'.join(self.copy_options)
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        copy_query = StageToRedshiftOperator.copy_query.format(table=self.table,
                                                               s3_path=s3_path,
                                                               access_key=credentials.access_key,
                                                               secret_key=credentials.secret_key,
                                                               data_format=self.data_format,
                                                               format_type = self.format_type,
                                                               copy_options=copy_options)
        
        self.log.info(f'Executing copying data into table {self.table}')
        try:
            self.hook.run(copy_query)
            self.log.info(f"Finish copying data into table {self.table}")
        except:
            self.log.info(f"No data path")





