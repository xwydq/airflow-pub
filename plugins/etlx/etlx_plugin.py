# coding: utf8
from __future__ import absolute_import, unicode_literals

import requests
from functools import wraps

import airflow
from flask import Blueprint, request, jsonify, redirect, url_for, flash
from flask_admin import BaseView, expose
from airflow.www.app import csrf
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.db import provide_session

from datetime import datetime, timedelta
from sqlalchemy import (
    Column, Integer, String, DateTime, Text, Boolean, ForeignKey, PickleType,
    Index, BigInteger, Float, and_)
from flask_wtf import FlaskForm
from wtforms import Form, FieldList, FormField, IntegerField, StringField, \
    SubmitField, validators
import json
from etlx.models import IndxInfo
from dagx.models import DagxDag

def login_required(func):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if airflow.login:
            return airflow.login.login_required(func)(*args, **kwargs)
        return func(*args, **kwargs)
    return func_wrapper

tmplt_sql = r"""
create table {0}
(
    {1}
    {2}
    id bigint auto_increment primary key
);
"""

dag_json = {"dag": {"action": "", "conf": {"tasks": [
    {"name": "task2", "type": "sql", "upstreams": ["dag"], "conn_id": "local_mysql", "sql": "create table "}]},
                    "create_time": "", "crond": "*/5 * * * *", "end_date": "2019-09-26 19:28:05", "name": "dag33",
                    "owner": "admin", "start_date": "2019-09-16 19:28:05", "update_time": "", "uuid": "", "version": 0}}


class DimForm(Form):
    dim_name = StringField('维度指标', [validators.DataRequired(),
                                    validators.Length(min=4, max=25),
                                    validators.regexp('^[a-zA-Z][_a-zA-Z0-9]*$')])
    dim_label = StringField('指标解释')


class MainDimForm(FlaskForm):
    fds = FieldList(
        FormField(DimForm),
        min_entries=1,
        max_entries=50
    )


class MetricForm(Form):
    metric_name = StringField('度量指标')
    metric_label = StringField('指标解释')


class MainMetricForm(FlaskForm):
    fds = FieldList(
        FormField(MetricForm),
        min_entries=1,
        max_entries=50
    )


class IndxException(Exception):
    def __init__(self, error_code, error_desc=''):
        self.error_code = '{}'.format(error_code)
        self.error_desc = '{}'.format(error_desc)
        super(IndxException, self).__init__(self.error_code + self.error_desc)


class EtlxView(BaseView):
    @provide_session
    def query_ind(self, usr_name, ind_name, session=None):
        indx = session.query(IndxInfo).filter(
            and_(IndxInfo.owner == usr_name, IndxInfo.ind_name == ind_name, IndxInfo.is_delete == 0)).first()
        # if not indx:
        #     raise IndxException('QUERY_NOT_FOUND',
        #                         'owner: {}; ind_nam: {}'.format(usr_name, ind_name))
        return indx

    def query_idxs(self, session):
        r = []
        for d in session.query(IndxInfo).filter(IndxInfo.is_delete == 0
                ).order_by(IndxInfo.create_time.desc()).all():
            dag = d.to_json()
            r.append(dag)
        return r

    @expose('/')
    @expose('/list', methods=['GET', 'POST'])
    @login_required
    @provide_session
    def list(self, session=None):
        return self.render('list.html', idxs=self.query_idxs(session))

    @expose("/delete_dag")
    @login_required
    @provide_session
    def delete_dag(self, session=None):
        print('delete_idx: {}'.format(request.args))
        usr_name = request.args.get("usr_name", None)
        ind_name = request.args.get("ind_name", None)
        d = session.query(IndxInfo).filter(
            and_(IndxInfo.owner == usr_name, IndxInfo.ind_name == ind_name)).first()
        if d:
            d.is_delete = 1
            session.commit()
        return self.render('list.html', dags=self.query_idxs(session))

    @csrf.exempt
    @expose('/index', methods=['GET', 'POST'])
    # @login_required
    @provide_session
    def index(self, session=None):
        usr_name = request.args.get("usr_name", type=str)
        ind_name = request.args.get("ind_name", type=str)
        # para = json.loads(request.form.get('para'))
        print('===========usr_name, ind_name')
        print(usr_name, ind_name)

        if usr_name is not None and ind_name is not None:
            indx = self.query_ind(usr_name=usr_name, ind_name=ind_name)
            if not indx:
                para = json.loads(request.get_data().decode('utf8'))

                try:
                    json.loads(para['ind_metric'])
                except ValueError:
                    raise IndxException('ERR JSON FORMAT')

                d = IndxInfo()
                d.owner = usr_name
                d.ind_name = ind_name
                d.formula = para['formula']
                d.ind_metric = para['ind_metric']
                session.add(d)
                session.commit()

            indx = self.query_ind(usr_name=usr_name, ind_name=ind_name)
            ind_metric = indx.ind_metric
            ind_dim = indx.ind_dim

            ind_metric = json.loads(ind_metric)
            ind_dim = json.loads(ind_dim)
            # print(ind_dim, ind_metric)

            if not ind_dim:
                ind_dim = {'dim_demo': ''}
            form_dim = MainDimForm()
            form_metric = MainMetricForm()

            while len(form_dim.fds) > 0:
                form_dim.fds.pop_entry()

            while len(form_metric.fds) > 0:
                form_metric.fds.pop_entry()

            for k, v in ind_dim.items():
                f = DimForm()
                f.dim_name = k
                f.dim_label = v
                form_dim.fds.append_entry(f)

            for k, v in ind_metric.items():
                f = MetricForm()
                f.metric_name = k
                f.metric_label = v
                form_metric.fds.append_entry(f)

            return self.render(
                'index.html',
                usr_name=usr_name,
                ind_name=ind_name,
                form_dim=form_dim,
                form_metric=form_metric
            )
        else:
            # 创建用户+指标
            flash('参数不满足要求')
            return '参数不全'

    @csrf.exempt
    @expose('/update', methods=['GET', 'POST'])
    @provide_session
    def update(self, session=None):
        para = json.loads(request.form.get('para'))
        data = json.loads(request.form.get('data'))
        usr_name = para.get("usr_name")
        ind_name = para.get("ind_name")

        # 验证数据有效性
        if usr_name is not None and ind_name is not None:
            indx = self.query_ind(usr_name=usr_name, ind_name=ind_name)
            if indx:
                session.query(IndxInfo).filter(
                    and_(IndxInfo.owner == usr_name, IndxInfo.ind_name == ind_name, IndxInfo.is_delete == 0)).update(
                    {'ind_dim': json.dumps(data).encode('utf8').decode('unicode_escape')})
                session.commit()
                if indx.uuid:
                    ## update dagx conf
                    dag_info = session.query(DagxDag).filter(
                        and_(DagxDag.uuid == indx.uuid, DagxDag.is_delete == 0)).first()
                    if dag_info:
                        dag_conf = json.loads(dag_info.conf, strict=False)
                        indx = self.query_ind(usr_name=usr_name, ind_name=ind_name)
                        tab_nm = 'idx_' + usr_name + '_' + ind_name + datetime.now().strftime('%Y%m%d')
                        ind_metric = json.loads(indx.ind_metric)
                        ind_dim = json.loads(indx.ind_dim)
                        sql_dim, sql_mtric = '', ''
                        for k in ind_dim.keys():
                            sql_dim += '{0} float not null,'.format(k)
                        for k in ind_metric.keys():
                            sql_mtric += '{0} varchar(128) not null,'.format(k)
                        sql_create_task = tmplt_sql.format(tab_nm, sql_dim, sql_mtric)
                        dag_conf['tasks'][0]['sql'] = sql_create_task
                        session.query(DagxDag).filter(
                            and_(DagxDag.uuid == indx.uuid, DagxDag.is_delete == 0)).update(
                            {'conf': json.dumps(dag_conf).encode('utf8').decode('unicode_escape')})
                        session.commit()
                return jsonify({"code": "SUCCESS", "info": {"usr_name":usr_name, "ind_name":ind_name}})
        return jsonify({"code": "ERR", "info": "paras err"})

    @csrf.exempt
    @expose('/render_dag', methods=['GET', 'POST'])
    @provide_session
    def render_dag(self, session=None):
        para = json.loads(request.form.get('para'))

        usr_name = para.get("usr_name")
        ind_name = para.get("ind_name")

        if usr_name is not None and ind_name is not None:
            indx = self.query_ind(usr_name=usr_name, ind_name=ind_name)
            ind_metric = indx.ind_metric
            ind_dim = indx.ind_dim
            uuid = indx.uuid

            if uuid:
                return jsonify({"code": "SUCCESS", "info": {"uuid": uuid, "status": 1}}) # status: 0-新建；1-已经存在

            ind_metric = json.loads(ind_metric)
            ind_dim = json.loads(ind_dim)

            ## 拼接dag json
            # 拼接建表task SQL
            tab_nm = 'idx_' + usr_name + '_' + ind_name + datetime.now().strftime('%Y%m%d')
            sql_dim, sql_mtric = '', ''
            for k in ind_dim.keys():
                sql_dim += '{0} float not null,'.format(k)
            for k in ind_metric.keys():
                sql_mtric += '{0} varchar(128) not null,'.format(k)
            sql_create_task = tmplt_sql.format(tab_nm, sql_dim, sql_mtric)

            # dag json
            dag_json['dag']['conf']['tasks'][0]['name'] = 'tab_' + usr_name + '_' + ind_name
            dag_json['dag']['conf']['tasks'][0]['sql'] = sql_create_task
            dag_json['dag']['name'] = 'etlidx_' + usr_name + '_' + ind_name
            dag_json['dag']['start_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            dag_json['dag']['end_date'] = (datetime.now() + timedelta(days=10)).strftime('%Y-%m-%d %H:%M:%S')
            # print('======dag_json========')
            # print(dag_json)

            # 调用接口 dagxview/update_dag?
            headers = {'Content-Type': 'application/json'}
            req_dag = requests.post('http://127.0.0.1:8080/admin/dagxview/update_dag?', headers=headers, data=json.dumps(dag_json))

            # 更新表中UUID
            res_code = json.loads(req_dag.text)
            if res_code.get('code') == 'SUCCESS':
                uuid = res_code.get('data').get('uuid')
                session.query(IndxInfo).filter(
                    and_(IndxInfo.owner == usr_name, IndxInfo.ind_name == ind_name, IndxInfo.is_delete == 0)).update({'uuid': uuid})
                session.commit()
                return jsonify({"code": "SUCCESS", "info": {"uuid": uuid, "status": 0}}) # status: 0-新建；1-已经存在
        else:
            return jsonify({"code": "ERR", "info": "param error"})



etlx_v = EtlxView(
    category="Admin",
    name="ETL_指标")

etlx_bp = Blueprint(
    "etlx",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/etlx")

class EtlXPlugin(AirflowPlugin):
    name = "etl_idx_plugin"
    flask_blueprints = [etlx_bp]
    admin_views = [etlx_v]

