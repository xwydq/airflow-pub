from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
import json
import os.path
from dagx import settings as dcmp_settings




class CsvToSQLDB(BaseOperator):

    # template_fields = ('sql', 'partition', 'hive_table')
    # template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(
            self,
            upload_csvfile,
            upload_conn_id,
            upload_tbl_list,
            CSVDIR=dcmp_settings.CSV_DIR,
            *args, **kwargs):
        super(CsvToSQLDB, self).__init__(*args, **kwargs)
        self.upload_csvfile = upload_csvfile
        self.upload_conn_id = upload_conn_id
        self.upload_tbl_list = upload_tbl_list
        self.CSVDIR=CSVDIR

    def execute(self, context):
        ## read csv file
        csvfile = os.path.join(self.CSVDIR, self.upload_csvfile)
        csvdf = pd.read_csv(csvfile)

        ## convert nan type to None
        csvdf = csvdf.where((pd.notnull(csvdf)), None)
        # rows for insert
        csvdf_rows = list(csvdf.itertuples(index=False, name=None))

        self.log.info("Loading csv file into SQLDB")
        db_hook = BaseHook.get_hook(conn_id=self.upload_conn_id)

        # insert method1: insert_rows
        # db_hook.insert_rows(table=self.upload_tbl_list, rows=csvdf_rows)

        # insert method2: pd.to_sql
        extras = BaseHook.get_connection(conn_id=self.upload_conn_id).extra
        sql_conn = create_engine(db_hook.get_uri(), connect_args=json.loads(extras))
        csvdf.to_sql(self.upload_tbl_list, con=sql_conn, index=False, if_exists='append')
