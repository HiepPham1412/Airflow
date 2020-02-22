from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 data_insertion_query,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.data_insertion_query = data_insertion_query
        self.redshift_conn_id = redshift_conn_id
        

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f'Executing data insertion.')
        self.hook.run(self.data_insertion_query)
        self.log.info(f"Data insertion completed.")
        