# encoding: utf-8

# __author__ = "isys"
# __version__ = "1.0.0"

import logging
import json
import difflib
from functools import wraps
from collections import OrderedDict

import airflow
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from airflow.utils.db import provide_session
from flask import Blueprint, Markup, request, jsonify, flash
from flask_admin import BaseView, expose
from flask_admin.babel import gettext
from pygments import highlight, lexers
from pygments.formatters import HtmlFormatter

from flask_admin.contrib.fileadmin import FileAdmin
import os.path as op
from airflow import configuration

# path = op.join('/home/airflow/airflow', 'files')
path = configuration.conf.get('uploaded_dir', 'ADMIN_FILEPTH')

print(path)
from wtforms import (
    Form, SelectField, TextAreaField, PasswordField,
    StringField, validators)
from airflow.models.connection import Connection


def login_required(func):
    # when airflow loads plugins, login is still None.
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if airflow.login:
            return airflow.login.login_required(func)(*args, **kwargs)
        return func(*args, **kwargs)

    return func_wrapper


def get_current_user(raw=True):
    try:
        if raw:
            res = airflow.login.current_user.user
        else:
            res = airflow.login.current_user
    except Exception as e:
        res = None
    return res


# class FileManager(BaseView):
#     @expose("/")
#     @expose("/filemgr")
#     @login_required
#     def index(self, session=None):
#         return self.render("dcmp/index.html",
#                            can_access_approver=can_access_approver(),
#                            dcmp_dags=dcmp_dags,
#                            dcmp_dags_count=dcmp_dags_count,
#                            filter_groups=request_args_filter.filter_groups,
#                            active_filters=request_args_filter.active_filters,
#                            search=search, )


file_manager_view = FileAdmin(path, category="Admin", name="File Manager")

# dag_creation_manager_bp = Blueprint(
#     "dag_creation_manager_bp",
#     __name__,
#     template_folder="templates",
#     static_folder="static",
#     static_url_path="/static/dcmp"
# )


# class FileManagerPlugin(AirflowPlugin):
#     name = "file_manager"
#     operators = []
#     flask_blueprints = []
#     hooks = []
#     executors = []
#     admin_views = [file_manager_view]
#     menu_links = []
