# encoding: utf-8
import sys

sys.path.insert(0, '../../')
import logging
import os


from dagx import settings as dcmp_settings



class DAGTemplates(object):
    get_list = lambda x: x if x else []
    get_string = lambda x: x.strip() if x else ""
    get_int = lambda x: int(x) if x else 0
    get_bool_code_true = lambda x: True if x is not False else False
    get_bool_code_false = lambda x: False if x is not True else True

    def load_dag_template(template_name):
        logging.info("loading dag template: %s" % template_name)
        with open(os.path.join(dcmp_settings.DAG_CREATION_MANAGER_DAG_TEMPLATES_DIR, template_name + ".template"),
                  "r") as f:
            res = f.read()
        return res

    CONN_AF = dcmp_settings.CONN_AF
    CSV_DIR = dcmp_settings.CSV_DIR

    DAG_ITEMS = (("dag_name", get_string, True), ("cron", get_string, True), ("category", get_string, True),
                 ("retries", get_int, False), ("retry_delay_minutes", get_int, False),
                 ("email_on_failure", get_bool_code_true, False),
                 ("email_on_retry", get_bool_code_false, False), ("depends_on_past", get_bool_code_false, False),
                 ("concurrency", lambda x: int(x) if x else 16, False),
                 ("max_active_runs", lambda x: int(x) if x else 16, False),
                 ("add_start_task", get_bool_code_false, False), ("add_end_task", get_bool_code_false, False),
                 ("skip_dag_not_latest", get_bool_code_false, False),
                 ("skip_dag_on_prev_running", get_bool_code_false, False),
                 ("email_on_skip_dag", get_bool_code_false, False), ("emails", get_string, False),
                 ("start_date", get_string, False),
                 ("end_date", get_string, False))
    TASK_ITEMS = (("task_name", get_string, True), ("task_type", get_string, True), ("command", get_string, False),
                  ("priority_weight", get_int, False), ("upstreams", get_list, False),
                  ("queue_pool", get_string, False),
                  ("task_category", get_string, False),
                  ("conn_id", get_string, False), ("conn_type", get_string, False),
                  ("src_conn_id", get_string, False), ("dst_conn_id", get_string, False),
                  ("dst_tab_name", get_string, False),
                  ("upload_csvfile", get_string, False), ("upload_conn_id", get_string, False),
                  ("upload_tbl_list", get_string, False),
                  )

    TASK_EXTRA_ITEMS = (
    ("retries", get_int, "retries=%s,"), ("retry_delay_minutes", get_int, "retry_delay=timedelta(minutes=%s),"),)

    DAG_CODE_TEMPLATE = load_dag_template("dag_code")

    BASE_TASK_CODE_TEMPLATE = r"""%(before_code)s
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

    DUMMY_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "DummyOperator",
        "operator_code": "", }

    BASH_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "BashOperator",
        "operator_code": r"""
    bash_command=r'''%(processed_command)s ''',
""", }

    PYTHON_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": """
def %(task_name)s_worker(ds, **context):
%(processed_command)s
    return None
""",
        "operator_name": "PythonOperator",
        "operator_code": r"""
    provide_context=True,
    python_callable=%(task_name)s_worker,
""", }

    STREAM_CODE_TEMPLATE = """
_["%(task_name)s"] << _["%(upstream_name)s"]
"""

    ## add sql operator TEMPLATE
    # mysql
    MySQL_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "MySqlOperator",
        "operator_code": r"""
        mysql_conn_id="%(conn_id)s",
        autocommit=True,
        sql=r'''
    %(processed_command)s
    ''',
    """, }

    # oracle
    Oracle_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "OracleOperator",
        "operator_code": r"""
        oracle_conn_id="%(conn_id)s",
        autocommit=True,
        sql=r'''
    %(processed_command)s
    ''',
    """, }

    # mssql
    MSSQL_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "MsSqlOperator",
        "operator_code": r"""
        mssql_conn_id="%(conn_id)s",
        autocommit=True,
        sql=r'''
    %(processed_command)s
    ''',
    """, }

    # Postgres
    Postgres_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "PostgresOperator",
        "operator_code": r"""
        postgres_conn_id="%(conn_id)s",
        autocommit=True,
        sql=r'''
    %(processed_command)s
    ''',
    """, }

    # hive
    HQL_TASK_CODE_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "HiveOperator",
        "operator_code": r"""
        hive_cli_conn_id="%(conn_id)s",
        mapred_job_name="%(task_name)s",
        mapred_queue=%(mapred_queue_code)s,
        hql=r'''
    %(processed_command)s
    ''',
    """, }

    MySqlToHiveTransfer_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "MySqlToHiveTransfer",
        "operator_code": r"""
        sql=r'''
%(processed_command)s
''',
        hive_table="%(dst_tab_name)s",
        create=False,
        recreate=False,
        partition=None,
        delimiter=chr(1),
        mysql_conn_id="%(src_conn_id)s",
        hive_cli_conn_id="%(dst_conn_id)s",
        tblproperties=None
    """, }

    MsSqlToHiveTransfer_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "MsSqlToHiveTransfer",
        "operator_code": r"""
            sql=r'''
    %(processed_command)s
    ''',
            hive_table="%(dst_tab_name)s",
            create=False,
            recreate=False,
            partition=None,
            delimiter=chr(1),
            mssql_conn_id="%(src_conn_id)s",
            hive_cli_conn_id="%(dst_conn_id)s",
            tblproperties=None
        """, }

    HiveToMySqlTransfer_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "HiveToMySqlTransfer",
        "operator_code": r"""
            sql=r'''
    %(processed_command)s
    ''',
            mysql_table="%(dst_tab_name)s",
            bulk_load=False,
            mysql_preoperator=None,
            mysql_postoperator=None,
            hiveserver2_conn_id="%(src_conn_id)s",
            mysql_conn_id="%(dst_conn_id)s",
        """, }

    OracleToOracleTransfer_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "OracleToOracleTransfer",
        "operator_code": r"""
            source_sql=r'''
    %(processed_command)s
    ''',
            destination_table="%(dst_tab_name)s",
            source_sql_params=None,
            rows_chunk=5000,
            oracle_source_conn_id="%(src_conn_id)s",
            oracle_destination_conn_id="%(dst_conn_id)s",
        """, }

    GenericTransfer_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "GenericTransfer",
        "operator_code": r"""
            sql=r'''
    %(processed_command)s
    ''',
            destination_table="%(dst_tab_name)s",
            preoperator=None,
            source_conn_id="%(src_conn_id)s",
            destination_conn_id="%(dst_conn_id)s",
        """, }

    ## upload csv file
    CsvToSQLDB_TEMPLATE = BASE_TASK_CODE_TEMPLATE % {
        "before_code": "",
        "operator_name": "CsvToSQLDB",
        "operator_code": r"""
            upload_csvfile="%(upload_csvfile)s",
            upload_conn_id="%(upload_conn_id)s",
            upload_tbl_list="%(upload_tbl_list)s",
        """, }

    TASK_TYPE_TO_TEMPLATE = {
        "bash": BASH_TASK_CODE_TEMPLATE,
        "dummy": DUMMY_TASK_CODE_TEMPLATE,
        "hql": HQL_TASK_CODE_TEMPLATE,
        "python": PYTHON_TASK_CODE_TEMPLATE,
        ## add sql operator dict
        "sqlOp_mysql": MySQL_TASK_CODE_TEMPLATE,
        "sqlOp_oracle": Oracle_TASK_CODE_TEMPLATE,
        "sqlOp_mssql": MSSQL_TASK_CODE_TEMPLATE,
        "sqlOp_postgres": Postgres_TASK_CODE_TEMPLATE,
        "sqlOp_hiveserver2": HQL_TASK_CODE_TEMPLATE,
        ## add sql transfer dict
        "mysql_to_hive": MySqlToHiveTransfer_TEMPLATE,
        "mssql_to_hive": MsSqlToHiveTransfer_TEMPLATE,
        "hive_to_mysql": HiveToMySqlTransfer_TEMPLATE,
        "oracle_to_oracle": OracleToOracleTransfer_TEMPLATE,
        "generic_transfer": GenericTransfer_TEMPLATE,
        ## upload_csvfile
        "data_upload": CsvToSQLDB_TEMPLATE,
    }
