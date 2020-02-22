from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_table_query = 'TRUNCATE TABLE {table};'
    insertion_query = 'INSERT INTO {table} \n'

    @apply_defaults
    def __init__(self,
                 data_selection_query,
                 table,
                 truncate_table=True,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.data_selection_query = data_selection_query
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.truncate_table = truncate_table

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.full_insertion_query = LoadDimensionOperator.insertion_query.format(table=self.table) + self.data_selection_query
        
        if self.truncate_table:
            self.log.info(f'Execute truncating {self.table} command...')
            self.hook.run(LoadDimensionOperator.truncate_table_query.format(table=self.table))
            self.log.info(f'Table {self.table} truncated...')
        
        self.log.info(f'Executing data insertion to table {self.table}.')
        self.hook.run(self.full_insertion_query)
        self.log.info(f'Data insertion in table {self.table} completed')
        
        
