# encoding: utf-8
import sys

sys.path.insert(0, '../')
import logging
import shutil
import re
import os
import json
from time import sleep
from copy import deepcopy
from datetime import datetime, timedelta
from tempfile import mkdtemp
from collections import OrderedDict

from croniter import croniter
from airflow import configuration
from airflow.utils.db import provide_session
from airflow.models import TaskInstance

from dagx import settings as dcmp_settings
# from dagx.models import DcmpDag
from dagx.models import DagxDag
from dagx.utils import create_dagbag_by_dag_code

from airflow.models.connection import Connection
from sqlalchemy import create_engine
import pandas as pd
from dagx.dag_templates.operator_template import DAGTemplates
import glob
from collections import OrderedDict
from itertools import islice

class DAGConverter(object):
    def __init__(self, lastminutes=100):
        self.lastminutes = lastminutes
        self.CONN_AF = dcmp_settings.CONN_AF

    JOB_NAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]+$")

    def check_job_name(self, job_name):
        return bool(self.JOB_NAME_RE.match(job_name))


    # 扫描结果
    def scan_result(self, uuid, status=1, desc=None):
        res_df = pd.DataFrame({
            'uuid': [uuid],
            'status': [status],
            'scan_time': [datetime.now()],
            'desc': [desc]
        })
        res_df.to_sql('dagx_conf2py', con=self.CONN_AF, if_exists='append', index=False)

    # 获取已经生成的dag py中数据表没有的记录
    @provide_session
    def get_deleted_dagpy(self, session=None):
        dagpath = dcmp_settings.DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER
        daglist_file = [f for f in glob.glob(dagpath + "**/*.py", recursive=False)]
        daglist_file = [os.path.splitext(os.path.basename(f))[0] for f in daglist_file]

        daglist_intab = session.query(DagxDag).with_entities(DagxDag.name).all()
        daglist_intab = [d[0] for d in daglist_intab]

        daglist_del = list(set(daglist_file) - set(daglist_intab))
        return daglist_del



    # 获取需要渲染的conf
    @provide_session
    def get_dagconfs_render(self, session=None):
        confs = OrderedDict()
        # dag_confs = session.query(DagxDag).all()
        # dag_confs = session.query(DagxDag).filter(DagxDag.is_delete == 0)
        dag_confs = session.query(DagxDag).filter(DagxDag.update_time >= datetime.now() - timedelta(minutes=self.lastminutes))

        for dag_conf in dag_confs:
            conf = json.loads(dag_conf.conf)
            if conf:
                conf['uuid'] = dag_conf.uuid
                conf['start_date'] = dag_conf.start_date.strftime("%Y-%m-%d %H:%M:%S")
                conf['end_date'] = dag_conf.end_date.strftime("%Y-%m-%d %H:%M:%S")
                conf['cron'] = dag_conf.crond
                conf['owner'] = dag_conf.owner
                conf['dag_name'] = dag_conf.name
                conf['op_type'] = 'delete' if dag_conf.is_delete else ('new' if dag_conf.create_time == dag_conf.update_time else 'update')
                conf['cron'] = conf['cron'] if conf['cron'] else 'None'

                conf['emails'] = ''
                conf['email_on_skip_dag'] = False
                conf['skip_dag_on_prev_running'] = False
                conf['skip_dag_not_latest'] = False
                conf['add_end_task'] = False
                conf['add_start_task'] = False
                conf['email_on_failure'] = False
                conf['email_on_retry'] = False
                conf['depends_on_past'] = False
                conf['category'] = 'default'
                conf['retries'] = 1
                conf['retry_delay_minutes'] = 5
                conf['concurrency'] = 1
                conf['max_active_runs'] = 1


                for task in conf["tasks"]:
                    ## 修改 dict key key: name –> task_name; type –> task_type;  sql –> command
                    task['task_name'] = task.pop('name') if 'name' in task else None
                    task['task_type'] = task.pop('type') if 'type' in task else None
                    task['priority_weight'] = 0

                    ## type TASK_TYPES = ["bash", "python", "sql_operate", "data_transfer", "data_upload"]
                    task_type_val = task.get('task_type', '')
                    if task_type_val == 'bash':
                        task['command'] = task.pop('bash_command') if 'bash_command' in task else ''
                    elif task_type_val == 'python':
                        task['command'] = task.pop('python_callable') if 'python_callable' in task else ''
                    elif task_type_val == 'sql':
                        task['task_type'] = 'sql_operate'
                        task['command'] = task.pop('sql') if 'sql' in task else ''
                    elif task_type_val == 'copy': #key: src_sql –> command;  dst_table –> dst_tab_name
                        task['task_type'] = 'data_transfer'
                        task['command'] = task.pop('src_sql') if 'src_sql' in task else ''
                        # task['command'] = ''
                        task['dst_tab_name'] = task.pop('dst_table') if 'dst_table' in task else ''
                    elif task_type_val == 'upload': #  conn_id–> upload_conn_id;  table–> upload_tbl_list;  file–> upload_csvfile
                        task['task_type'] = 'data_upload'
                        task['upload_conn_id'] = task.pop('conn_id') if 'conn_id' in task else ''
                        task['upload_csvfile'] = task.pop('file') if 'file' in task else ''
                        task['upload_tbl_list'] = task.pop('table') if 'table' in task else ''
                        # task['upload_csvfile'] = ''

                    ## 'upstreams': ['dag'], 删除
                    if 'dag' in task['upstreams']:
                        task['upstreams'].remove('dag')
                confs[dag_conf.name] = conf
        return confs


    # 删除dag
    @provide_session
    def delete_dag(self, session=None, dag_name=None):
        dag_del = session.query(DagxDag).filter(DagxDag.name == dag_name).first()
        if dag_del:
            # dag_del.is_delete = 1
            # session.commit()
            ## 删除文件
            dag_py = os.path.join(dcmp_settings.DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER, dag_del.name + '.py')
            if os.path.exists(dag_py):
                os.remove(dag_py)
            # else:
            #     raise ("Can not delete the file as it doesn't exists")

            ## 删除 airflow记录
            with dcmp_settings.CONN_AF.connect() as con:
                con.execute('delete from airflow.xcom where dag_id = "%s"' % dag_del.name)
                con.execute('delete from airflow.task_instance where dag_id = "%s"' % dag_del.name)
                con.execute('delete from airflow.sla_miss where dag_id = "%s"' % dag_del.name)
                con.execute('delete from airflow.log where dag_id = "%s"' % dag_del.name)
                con.execute('delete from airflow.job where dag_id = "%s"' % dag_del.name)
                con.execute('delete from airflow.dag_run where dag_id = "%s"' % dag_del.name)
                con.execute('delete from airflow.log where dag_id = "%s"' % dag_del.name)
                con.execute('delete from airflow.dag where dag_id = "%s"' % dag_del.name)


    ## TASK conf有效性检验
    def check_task_dict(self, dag_uuid, task_dict, strict=False):
        task_name = task_dict.get("task_name")
        if not task_name:
            desc_words = "task check: task name required"
            self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
            return False
        task_name = DAGTemplates.get_string(task_name)
        if not self.check_job_name(task_name):
            desc_words = "task check: task_name para '%s' invalid" % task_name
            self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
            return False

        task_type = task_dict.get("task_type")
        if task_type == 'sql_operate':
            conn_id = task_dict.get("conn_id", '')
            if not conn_id:
                desc_words = "task check: task_name '%s' para conn_id '%s' param invalid" % (task_name, conn_id)
                self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
                return False

        if task_type == 'data_transfer':
            src_conn_id = task_dict.get("src_conn_id", '')
            dst_conn_id = task_dict.get("dst_conn_id", '')
            dst_tab_name = task_dict.get("dst_tab_name", '')
            dst_tab_name = DAGTemplates.get_string(dst_tab_name)

            if not dst_tab_name or not src_conn_id or not dst_conn_id:
                desc_words = "task check: task_name '%s' paras src_conn_id '%s'/ dst_conn_id '%s'/ dst_tab_name '%s' params invalid" % (task_name, src_conn_id, dst_conn_id, dst_tab_name)
                self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
                return False
            if not self.check_job_name(dst_tab_name):
                desc_words = "task check: task_name '%s' paras dst_tab_name para '%s' invalid" % (task_name, dst_tab_name)
                self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
                return False

        if task_type == 'data_upload':
            upload_csvfile = task_dict.get("upload_csvfile", '')
            upload_conn_id = task_dict.get("upload_conn_id", '')
            upload_tbl_list = task_dict.get("upload_tbl_list", '')
            upload_tab_name = DAGTemplates.get_string(upload_tbl_list)

            if not upload_tab_name or not upload_conn_id or not upload_csvfile:
                desc_words = "task check: task_name '%s' paras upload_conn_id '%s'/ upload_csvfile '%s'/ upload_tab_name '%s' params invalid" % (task_name, upload_conn_id, upload_csvfile, upload_tab_name)
                self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
                return False
            if not self.check_job_name(upload_tab_name):
                desc_words = "task check: task_name '%s' paras table_name para '%s' invalid" % (task_name, upload_tab_name)
                self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
                return False
        desc_words = "task check: task_name '%s' ok" % task_name
        self.scan_result(uuid=dag_uuid, status=1, desc=desc_words)
        return True

    ## TASK conf 取值的格式转换
    def clean_task_dict(self, task_dict, strict=False):
        task_name = task_dict.get("task_name")
        task_res = {}
        for key, trans_func, required in DAGTemplates.TASK_ITEMS:
            value = task_dict.get(key)
            if required and not value:
                raise ValueError("task %s params %s required" % (task_name, key))
            if strict and key in ["queue_pool"] and not value:
                raise ValueError("task %s params %s required" % (task_name, key))
            value = trans_func(value)
            task_res[key] = value
        for key, trans_func, _ in DAGTemplates.TASK_EXTRA_ITEMS:
            value = task_dict.get(key)
            if value is not None and value != "":
                value = trans_func(value)
                task_res[key] = value
        return task_res

    ## check dag (and task in dag)
    def check_dag_dict(self, dag_dict, strict=False):
        dag_uuid = dag_dict['uuid']
        if not dag_dict or not isinstance(dag_dict, dict):
            raise ValueError("dags required")

        task_dicts = dag_dict.get("tasks", [])
        if not task_dicts or not isinstance(task_dicts, list):
            # raise ValueError("tasks required")
            desc_words = "dag: tasks required"
            self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
            return False

        dag_res = {}
        for key, trans_func, required in DAGTemplates.DAG_ITEMS:
            value = dag_dict.get(key)
            if required and not value:
                # raise ValueError("dag params %s required" % key)
                desc_words = "dag: dag params %s required" % key
                self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
                return False
            value = trans_func(value)
            dag_res[key] = value

        dag_name = dag_res["dag_name"]
        if not self.check_job_name(dag_name):
            # raise ValueError("dag name invalid")
            desc_words = "dag: dag name invalid"
            self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
            return False

        cron = dag_res["cron"]
        if cron == "None":
            pass
        else:
            try:
                croniter(cron)
            except Exception as e:
                # raise ValueError("dag params cron invalid")
                desc_words = "dag: dag params cron invalid"
                self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
                return False

        task_names = []
        tasks_res = []
        for task_dict in task_dicts:
            task_checked = self.check_task_dict(dag_uuid, task_dict)
            if not task_checked:
                return False
            task_res = self.clean_task_dict(task_dict, strict=strict)
            task_name = task_res["task_name"]
            if task_name in task_names:
                # raise ValueError("task %s name duplicated" % task_name)
                desc_words = "dag: task %s name duplicated" % task_name
                self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
                return False
            task_names.append(task_res["task_name"])
            tasks_res.append(task_res)

        for task_res in tasks_res:
            for upstream in task_res["upstreams"]:
                if upstream not in task_names or upstream == task_res["task_name"]:
                    # raise ValueError("task %s upstream %s invalid" % (task_res["task_name"], upstream))
                    desc_words = "dag: task %s upstream %s invalid" % (task_res["task_name"], upstream)
                    self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
                    return False
        dag_res["tasks"] = tasks_res
        dag_res["uuid"] = dag_uuid
        dagbag = self.create_dagbag_by_conf(dag_res)
        if dagbag.import_errors:
            # raise ImportError(list(dagbag.import_errors.items())[0][1])
            desc_words = str(ImportError(list(dagbag.import_errors.items())[0][1]))
            self.scan_result(uuid=dag_uuid, status=0, desc=desc_words)
            return False

        return True


    def dict_to_json(self, dag_dict, strict=False):
        if not dag_dict or not isinstance(dag_dict, dict):
            raise ValueError("dags required")

        task_dicts = dag_dict.get("tasks", [])
        if not task_dicts or not isinstance(task_dicts, list):
            raise ValueError("tasks required")

        dag_res = {}
        for key, trans_func, required in DAGTemplates.DAG_ITEMS:
            value = dag_dict.get(key)
            if required and not value:
                raise ValueError("dag params %s required" % key)
            value = trans_func(value)
            dag_res[key] = value

        dag_name = dag_res["dag_name"]
        if not self.check_job_name(dag_name):
            raise ValueError("dag name invalid")

        cron = dag_res["cron"]
        if cron == "None":
            pass
        else:
            try:
                croniter(cron)
            except Exception as e:
                raise ValueError("dag params cron invalid")

        task_names = []
        tasks_res = []
        for task_dict in task_dicts:
            task_res = self.clean_task_dict(task_dict, strict=strict)
            task_name = task_res["task_name"]
            if task_name in task_names:
                raise ValueError("task %s name duplicated" % task_name)
            task_names.append(task_res["task_name"])
            tasks_res.append(task_res)

        for task_res in tasks_res:
            for upstream in task_res["upstreams"]:
                if upstream not in task_names or upstream == task_res["task_name"]:
                    raise ValueError("task %s upstream %s invalid" % (task_res["task_name"], upstream))

        dag_res["tasks"] = tasks_res
        return dag_res

    @provide_session
    def get_conn_db_type(self, conn_id, session=None):
        dbs = session.query(Connection).filter(
            Connection.conn_id == conn_id,
        ).first()
        session.expunge_all()
        # db_choices = [db.conn_id for db in dbs if db.get_hook()]
        if dbs:
            db_type = dbs.conn_type
            return db_type
        return None

    def render_confs(self, confs):
        confs = deepcopy(confs)
        now = datetime.now()
        dag_codes = []
        task_catgorys_dict = {
            "default": {"order": str(0), "fgcolor": "#f0ede4"}
        }
        for i, category_data in enumerate(dcmp_settings.DAG_CREATION_MANAGER_TASK_CATEGORYS):
            key, fgcolor = category_data
            task_catgorys_dict[key] = {"order": str(i + 1), "fgcolor": fgcolor}

        for dag_name, conf in confs.items():
            emails = [email.strip() for email in conf["emails"].split(",") if
                      email.strip()] or dcmp_settings.DAG_CREATION_MANAGER_DEFAULT_EMAILS
            conf["email_code"] = json.dumps(emails)
            # conf["email_code"] = json.dumps('zlj@isyscore.com')

            if not conf.get("owner"):
                conf["owner"] = "airflow"

            task_names = [task["task_name"] for task in conf["tasks"]]

            def get_task_name(origin_task_name):
                task_name = origin_task_name
                for i in range(10000):
                    if task_name in task_names:
                        task_name = "%s_%s" % (origin_task_name, i)
                    else:
                        break
                else:
                    task_name = None
                return task_name

            if conf["add_start_task"]:
                task_name = get_task_name("start")
                if task_name:
                    for task in conf["tasks"]:
                        if not task["upstreams"]:
                            task["upstreams"] = [task_name]
                    conf["tasks"].append(self.clean_task_dict({
                        "task_name": task_name,
                        "task_type": "dummy",
                    }))

            if conf["add_end_task"]:
                task_name = get_task_name("end")
                if task_name:
                    root_task_names = set(task_names)
                    for task in conf["tasks"]:
                        root_task_names -= set(task["upstreams"])
                    conf["tasks"].append(self.clean_task_dict({
                        "task_name": task_name,
                        "task_type": "dummy",
                        "upstreams": root_task_names,
                    }))

            if conf["skip_dag_not_latest"] or conf["skip_dag_on_prev_running"]:
                task_name = []
                if conf["skip_dag_not_latest"]:
                    task_name.append("not_latest")
                if conf["skip_dag_on_prev_running"]:
                    task_name.append("when_previous_running")
                task_name = "_or_".join(task_name)
                task_name = "skip_dag_" + task_name
                task_name = get_task_name(task_name)
                if task_name:
                    command = """
skip = False
if context['dag_run'] and context['dag_run'].external_trigger:
    logging.info('Externally triggered DAG_Run: allowing execution to proceed.')
    return True
"""
                    if conf["skip_dag_not_latest"]:
                        command += """
if not skip:
    now = datetime.now()
    left_window = context['dag'].following_schedule(context['execution_date'])
    right_window = context['dag'].following_schedule(left_window)
    logging.info('Checking latest only with left_window: %s right_window: %s now: %s', left_window, right_window, now)

    if not left_window < now <= right_window:
        skip = True
"""

                    if conf["skip_dag_on_prev_running"]:
                        command += """
if not skip:
    session = settings.Session()
    count = session.query(DagRun).filter(
        DagRun.dag_id == context['dag'].dag_id,
        DagRun.state.in_(['running']),
    ).count()
    session.close()
    logging.info('Checking running DAG count: %s' % count)
    skip = count > 1
"""

                    if conf["email_on_skip_dag"]:
                        command += """
if skip:
    send_alert_email("SKIP", context)
"""

                    command += """
return not skip
"""

                    for task in conf["tasks"]:
                        if not task["upstreams"]:
                            task["upstreams"] = [task_name]
                    conf["tasks"].append(self.clean_task_dict({
                        "task_name": task_name,
                        "task_type": "short_circuit",
                        "command": command,
                    }))

            check_list = []
            for task in conf["tasks"]:
                check_list.append(self.check_task_dict(conf['uuid'], task))
                extra_params = []
                for key, trans_func, template in DAGTemplates.TASK_EXTRA_ITEMS:
                    value = task.get(key)
                    if value is None:
                        continue
                    value = trans_func(value)
                    extra_params.append(template % value)
                task["extra_params"] = "".join(extra_params)

            if all(check_list):
                self.scan_result(uuid=conf['uuid'], status=1, desc='dag ok')
            else:
                self.scan_result(uuid=conf['uuid'], status=0, desc='some task conf invalid')

            cron = conf["cron"]
            if cron is "None":
                conf["start_date_code"] = now.strftime(
                    'datetime.strptime("%Y-%m-%d %H:%M:%S", "%%Y-%%m-%%d %%H:%%M:%%S")')
                conf["end_date_code"] = "None"
                conf["cron_code"] = "None"
            else:
                cron_instance = croniter(cron, now)
                start_date = cron_instance.get_prev(datetime)
                conf["start_date_code"] = start_date.strftime(
                    'datetime.strptime("%Y-%m-%d %H:%M:%S", "%%Y-%%m-%%d %%H:%%M:%%S")')
                conf["end_date_code"] = "None"
                conf["cron_code"] = "'%s'" % cron

            if conf["start_date"]:
                conf["start_date_code"] = 'datetime.strptime("%s", "%%Y-%%m-%%d %%H:%%M:%%S")' % conf["start_date"]

            if conf["end_date"]:
                conf["end_date_code"] = 'datetime.strptime("%s", "%%Y-%%m-%%d %%H:%%M:%%S")' % conf["end_date"]

            dag_code = DAGTemplates.DAG_CODE_TEMPLATE % conf

            task_codes = []
            stream_codes = []
            for task in conf["tasks"]:
                # queue_pool = dcmp_settings.DAG_CREATION_MANAGER_QUEUE_POOL_DICT.get(task["queue_pool"])
                queue_pool = None
                if queue_pool:
                    queue, pool = queue_pool
                    task["queue_code"] = "'%s'" % queue
                    task["pool_code"] = "'%s'" % pool
                else:
                    task["queue_code"] = "'%s'" % configuration.get("celery", "default_queue")
                    task["pool_code"] = "None"

                task["task_category"] = task.get("task_category", "default")
                task_category = task_catgorys_dict.get(task["task_category"], None)
                if not task_category:
                    task["task_category"] = "default"
                    task_category = task_catgorys_dict["default"]
                task["task_category_fgcolor"] = task_category["fgcolor"]
                task["task_category_order"] = task_category["order"]

                if task["task_type"] in ["python", "short_circuit"]:
                    task["processed_command"] = "\n".join(map(lambda x: "    " + x, task["command"].split("\n")))
                else:
                    task["processed_command"] = task.get("command", '')

                if task["task_type"] == "hql":
                    mapred_queue_code = dcmp_settings.DAG_CREATION_MANAGER_QUEUE_POOL_MR_QUEUE_DICT.get(
                        task["queue_pool"], None)
                    if mapred_queue_code:
                        mapred_queue_code = '"%s"' % mapred_queue_code
                    else:
                        mapred_queue_code = "None"
                    task["mapred_queue_code"] = mapred_queue_code

                task_template = DAGTemplates.TASK_TYPE_TO_TEMPLATE.get(task["task_type"])

                ## 判断新类型，根据不同的数据库选择模板
                # if task["task_type"] == "new_task"
                if task["task_type"] == "sql_operate":
                    # conn_id sql type
                    db_type = self.get_conn_db_type(conn_id=task["conn_id"])
                    if db_type is not None:
                        db_type_template_name = 'sqlOp_' + db_type
                        task_template = DAGTemplates.TASK_TYPE_TO_TEMPLATE.get(db_type_template_name)

                if task["task_type"] == "data_transfer":
                    # conn_id sql type
                    source_db_type = self.get_conn_db_type(conn_id=task["src_conn_id"])
                    destination_db_type = self.get_conn_db_type(conn_id=task["dst_conn_id"])
                    if source_db_type == 'mysql' and destination_db_type == 'hiveserver2':
                        db_type_template_name = 'mysql_to_hive'
                    elif source_db_type == 'mssql' and destination_db_type == 'hiveserver2':
                        db_type_template_name = 'mssql_to_hive'
                    elif source_db_type == 'hiveserver2' and destination_db_type == 'mysql':
                        db_type_template_name = 'hive_to_mysql'
                    elif source_db_type == 'oracle' and destination_db_type == 'oracle':
                        db_type_template_name = 'oracle_to_oracle'
                    else:
                        db_type_template_name = 'generic_transfer'
                    task_template = DAGTemplates.TASK_TYPE_TO_TEMPLATE.get(db_type_template_name)

                if task_template:
                    task_codes.append(task_template % task)
                else:
                    continue

                for upstream in task["upstreams"]:
                    stream_code = DAGTemplates.STREAM_CODE_TEMPLATE % {
                        "task_name": task["task_name"],
                        "upstream_name": upstream,
                    }
                    stream_codes.append(stream_code)

            dag_code = "%s\n%s\n%s" % (dag_code, "\n".join(task_codes), "\n".join(stream_codes))
            dag_codes.append((dag_name, dag_code))
        return dag_codes

    def create_dagbag_by_conf(self, conf):
        _, dag_code = self.render_confs({conf["dag_name"]: conf})[0]
        return create_dagbag_by_dag_code(dag_code)

    def clean_dag_dict(self, dag_dict, strict=False):
        conf = self.dict_to_json(dag_dict, strict=strict)
        dagbag = self.create_dagbag_by_conf(conf)
        if dagbag.import_errors:
            raise ImportError(list(dagbag.import_errors.items())[0][1])
        return conf

    def create_dag_by_conf(self, conf):
        return self.create_dagbag_by_conf(conf).dags[conf["dag_name"]]


    def refresh_dagxs(self, dag_codes):
        tmp_dir = mkdtemp(prefix="dagx_deployed_dags_")
        os.chmod(tmp_dir, 0o755)
        for dag_name, dag_code in dag_codes:
            with open(os.path.join(tmp_dir, dag_name + ".py"), "wb") as f:
                f.write(dag_code.encode("utf-8"))
        # 增量 append
        err = None
        for _ in range(3):
            if not os.path.exists(dcmp_settings.DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER):
                os.makedirs(dcmp_settings.DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER, exist_ok=True)
            try:
                shutil.copy(src=os.path.join(tmp_dir, dag_name + ".py"),
                            dst=os.path.join(dcmp_settings.DAG_CREATION_MANAGER_DEPLOYED_DAGS_FOLDER, dag_name + ".py"))
            except Exception as e:
                err = e
                sleep(1)
            else:
                shutil.rmtree(tmp_dir, ignore_errors=True)
                break
        else:
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise err



if __name__ == '__main__':
    dag_converter = DAGConverter(10)

    # 删除多余DAG py文件
    dagx_dels = dag_converter.get_deleted_dagpy()
    for d in dagx_dels:
        dag_converter.delete_dag(dag_name=d)

    # 待 render 的conf
    dagx_confs = dag_converter.get_dagconfs_render()

    dagx_len = len(dagx_confs)
    if dagx_len > 0:
        for dc in range(dagx_len):
            dagx_od = OrderedDict(islice(dagx_confs.items(), dc, dc+1))
            dagx_conf = list(dagx_od.values())[0]

            op_type = dagx_conf['op_type']
            if op_type == 'delete':
                dag_converter.delete_dag(dag_name=dagx_conf['dag_name'])
            else:
                ## check
                check_res = dag_converter.check_dag_dict(dagx_conf)
                ## clean

                if check_res:
                    ## dag_code
                    dagx_code = dag_converter.render_confs(dagx_od)

                    ## render
                    dag_converter.refresh_dagxs(dagx_code)
                else:
                    continue