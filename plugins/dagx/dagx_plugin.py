# coding: utf8 
from __future__ import absolute_import, unicode_literals

import os
from uuid import uuid1
from functools import wraps
from datetime import datetime, timedelta

from flask import Blueprint, request, jsonify, redirect, url_for
from flask_admin import BaseView, expose
from airflow.www.app import csrf
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session
from airflow.models.connection import Connection

from dagx.models import DagxDag, DagxConf2Py


class DagxException(Exception):
    def __init__(self, error_code, error_desc=''):
        self.error_code = '{}'.format(error_code)
        self.error_desc = '{}'.format(error_desc)
        super(DagxException, self).__init__(self.error_code + self.error_desc)


def catch_dagx_exception(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except DagxException as e:
            return jsonify({"code": e.error_code, "desc": e.error_desc})
        except Exception as e:
            return jsonify({"code": 'SERVICE_EXCEPTION', "desc": str(e)})
    return wrapper


def get_param(params, key, vtype=None, essential=False, default=None):
    value = params.get(key, None)
    if value == 'undefined':
        value = None
    if value == 'None':
        value = None
    if vtype is not None and value is not None:
        try:
            value = vtype(value)
        except Exception as e:
            raise DagxException('ERR_PARAM_TYPE_ERROR', key)
    if value is None and default is not None:
        value = default
    if value is None and essential:
        raise DagxException('ERR_MISSING_PARAM', key)
    return value


class DagxView(BaseView):
    def query_dag(self, session, uuid):
        dag = session.query(DagxDag).filter(DagxDag.uuid==uuid).first()
        if not dag:
            raise DagxException('ERR_DAG_NOT_FOUND', uuid)
        return dag


    def query_dags(self, session):
        r = []
        for d in session.query(DagxDag).filter(DagxDag.is_delete == 0
                ).order_by(DagxDag.create_time.desc()).all():
            dag = d.to_json()
            c = session.query(DagxConf2Py).filter(DagxConf2Py.uuid==d.uuid
                    ).order_by(DagxConf2Py.scan_time.desc()).first()
            if c:
                dag['status'] = c.status
                dag['status_name'] = '成功' if c.status == 1 else '失败'
                st = c.scan_time + timedelta(hours=8)
                #dag['scan_time'] = c.scan_time.strftime('%Y-%m-%d %H:%M:%S')
                dag['scan_time'] = st.strftime('%Y-%m-%d %H:%M:%S')
                dag['desc'] = c.desc
            r.append(dag)
        return r


    def get_dags_count(self, session):
        return len(session.query(DagxDag).all())


    def query_connections(self, session):
        return session.query(Connection).filter(Connection.conn_type.in_(
                ['mysql', 'mssql', 'postgres', 'hive', 'oracle'])).all()


    @expose("/")
    @expose("/index")
    @provide_session
    def index(self, session=None):
        return self.render('dagx/index.html', dags=self.query_dags(session))
    
    
    @expose("/delete_dag")
    @provide_session
    @catch_dagx_exception
    def delete_dag(self, session=None):
        print('>> delete_dag: {}'.format(request.args))
        uuid = request.args.get("uuid", None)
        # session.query(DagxDag).filter(DagxDag.uuid==uuid).delete()
        d = session.query(DagxDag).filter(DagxDag.uuid==uuid).first()
        if d:
            d.is_delete = 1
            session.commit()
        return self.render('dagx/index.html', dags=self.query_dags(session))

    
    @expose("/edit_dag")
    @provide_session
    @catch_dagx_exception
    def edit_dag(self, session=None):
        print('>> edit_dag: {}'.format(request.args))
        uuid = request.args.get("uuid", None)
        if uuid:
            dag = self.query_dag(session, uuid).to_json()
        else:
            dag = DagxDag().to_json()
            dag['name'] = 'dag{}'.format(self.get_dags_count(session) + 1)
        connections = []
        for c in self.query_connections(session):
            connections.append({"conn_id": c.conn_id})
        return self.render('dagx/edit_dag.html', dag=dag, 
                connections=connections, 
                files=self.get_files())


    @csrf.exempt
    @expose("/update_dag", methods=["GET", "POST"])
    @provide_session
    @catch_dagx_exception
    def update_dag(self, session=None):
        print('>> update_dag: {}'.format(request.get_json()))
        data = request.get_json()
        print(data)
        dag = get_param(data, 'dag', vtype=dict, essential=True)
        uuid = get_param(dag, 'uuid', default='')
        if uuid:
            d = self.query_dag(session, uuid)
        else:
            d = DagxDag()
            d.uuid = str(uuid1())
        err = d.from_json(dag)
        if err:
            raise DagxException('ERR_PARSE_FAILED', err)
        session.merge(d)
        session.commit()
        return jsonify({"code": "SUCCESS", "data": {"uuid": d.uuid}})

        
    def get_files(self):
        root = os.path.join(os.environ.get('AIRFLOW_HOME', ''), './files')
        files = []
        for n in os.listdir(root):
            t = n.rsplit('.', maxsplit=1)
            fmt = t[1].lower() if len(t) == 2 else ''
            path = os.path.join(root, n)
            if os.path.isfile(path) and fmt in ['csv']:
                files.append({'name': n, 'path': path})
        return files


dagx_v = DagxView(
        category="Admin", 
        name="DAGX")


dagx_bp = Blueprint(
        "dagx_bp",
        __name__, 
        template_folder="templates",
        static_folder="static", 
        static_url_path="/static/dagx")


class DagXPlugin(AirflowPlugin):
    name = "dag_extension_plugin"
    flask_blueprints = [dagx_bp]
    admin_views = [dagx_v]

