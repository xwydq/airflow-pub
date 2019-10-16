import ast
import codecs
import copy
import datetime as dt
from io import BytesIO
import itertools
import json
import logging
import math
import os
import traceback
from collections import defaultdict
from datetime import timedelta
from functools import wraps
from textwrap import dedent

import markdown
import pendulum
import sqlalchemy as sqla
from flask import (
    abort, jsonify, redirect, url_for, request, Markup, Response,
    current_app, render_template, make_response, send_file)
from flask import flash
from flask_admin import BaseView, expose, AdminIndexView
from flask_admin.actions import action
from flask_admin.babel import lazy_gettext
from flask_admin.contrib.sqla import ModelView
from flask_admin.form.fields import DateTimeField
from flask_admin.tools import iterdecode
from jinja2 import escape
from jinja2.sandbox import ImmutableSandboxedEnvironment
from past.builtins import basestring
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter
from sqlalchemy import or_, desc, and_, union_all
from wtforms import (
    Form, SelectField, TextAreaField, PasswordField,
    StringField, validators)

import airflow
from airflow import configuration as conf
from airflow import models
from airflow import settings
from airflow.api.common.experimental.mark_tasks import (set_dag_run_state_to_running,
                                                        set_dag_run_state_to_success,
                                                        set_dag_run_state_to_failed)
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, errors
from airflow.models import XCom, DagRun
from airflow.models.connection import Connection
from airflow.operators.subdag_operator import SubDagOperator
from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, SCHEDULER_DEPS
from airflow.utils import timezone
from airflow.utils.dates import infer_time_unit, scale_time_units, parse_execution_date
from airflow.utils.db import create_session, provide_session
from airflow.utils.helpers import alchemy_to_dict, render_log_filename
from airflow.utils.net import get_hostname
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow._vendor import nvd3
from airflow.www import utils as wwwutils
from airflow.www.forms import (DateTimeForm, DateTimeWithNumRunsForm,
                               DateTimeWithNumRunsWithDagRunsForm)
from airflow.www.validators import GreaterEqualThan



class QueryViewTest(wwwutils.DataProfilingMixin, BaseView):
    @provide_session
    def query(self, session=None):
        dbs = session.query(Connection).order_by(Connection.conn_id).all()
        session.expunge_all()
        db_choices = list(
            ((db.conn_id, db.conn_id) for db in dbs if db.get_hook()))

        conn_id = SelectField("Layout", choices=db_choices)
        form = {'conn_id':conn_id}
        session.commit()
        return form
