from airflow.models.connection import Connection
from airflow.utils.db import create_session, provide_session
# from itertools import combinations
# db_combs = list(combinations(db_types, 2))
from airflow.utils.decorators import apply_defaults

db_types = ["mysql", "oracle", "mssql", "postgres", "hiveserver2", ]
db_combs = [(di, dj) for di in db_types for dj in db_types]
db_combs = [di + '_to_' + dj for di in db_types for dj in db_types]

from airflow.operators.mysql_to_hive import MySqlToHiveTransfer
from airflow.operators.mssql_to_hive import MsSqlToHiveTransfer
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.contrib.operators.oracle_to_oracle_transfer import OracleToOracleTransfer

from airflow.operators.generic_transfer import GenericTransfer

class DataTransfer():
    @apply_defaults
    def __init__(
            self,
            sql,
            destination_table,
            source_conn_id,
            destination_conn_id, ):
        self.sql = sql
        self.destination_table = destination_table
        self.source_conn_id = source_conn_id
        self.destination_conn_id = destination_conn_id
        self.BASE_TASK_CODE_TEMPLATE = r"""%(before_code)s
        _["%%(task_name)s"] = %(operator_name)s(
            task_id='%%(task_name)s',
        %(operator_code)s
            priority_weight=%%(priority_weight)s,
            queue=%%(queue_code)s,
            pool=%%(pool_code)s,
            dag=dag,
            %%(extra_params)s)

        _["%%(task_name)s"].category = {
            "name": r'''%%(task_category)s''',
            "fgcolor": r'''%%(task_category_fgcolor)s''',
            "order": %%(task_category_order)s,
        }
        """

    @provide_session
    def get_conn_db_type(self, conn_id, session=None):
        dbs = session.query(Connection).filter(
            Connection.conn_id == conn_id,
        ).first()
        session.expunge_all()
        if dbs:
            db_type = dbs.conn_type
            return db_type
        return None

    def match_datatransfer_operater(self):
        source_db_type = self.get_conn_db_type(self.source_conn_id)
        destination_db_type = self.get_conn_db_type(self.destination_conn_id)

        if source_db_type == 'mysql' and destination_db_type == 'hiveserver2':
            return MySqlToHiveTransfer(
                sql=self.sql,
                hive_table=self.destination_table,
                create=False,
                recreate=False,
                partition=None,
                delimiter=chr(1),
                mysql_conn_id=self.source_conn_id,
                hive_cli_conn_id=self.destination_conn_id,
                tblproperties=None
            )

        if source_db_type == 'mssql' and destination_db_type == 'hiveserver2':
            return MsSqlToHiveTransfer(
                sql=self.sql,
                hive_table=self.destination_table,
                create=False,
                recreate=False,
                partition=None,
                delimiter=chr(1),
                mysql_conn_id=self.source_conn_id,
                hive_cli_conn_id=self.destination_conn_id,
                tblproperties=None
            )

        if source_db_type == 'hiveserver2' and destination_db_type == 'mysql':
            return HiveToMySqlTransfer(
                sql=self.sql,
                mysql_table=self.destination_table,
                hiveserver2_conn_id=self.source_conn_id,
                mysql_conn_id=self.destination_conn_id,
                mysql_preoperator=None,
                mysql_postoperator=None,
                bulk_load=False
            )

        if source_db_type == 'oracle' and destination_db_type == 'oracle':
            return OracleToOracleTransfer(
                source_sql=self.sql,
                destination_table=self.destination_table,
                oracle_source_conn_id=self.source_conn_id,
                oracle_destination_conn_id=self.destination_conn_id,
                source_sql_params=None,
                rows_chunk=5000
            )

        return GenericTransfer(
            sql=self.sql,
            destination_table=self.destination_table,
            source_conn_id=self.source_conn_id,
            destination_conn_id=self.destination_conn_id,
            preoperator=None
        )
