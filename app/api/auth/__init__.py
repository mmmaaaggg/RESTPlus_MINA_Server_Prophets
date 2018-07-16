#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/6 13:44
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from flask_restplus import Namespace, Resource, fields, reqparse
from werkzeug.exceptions import BadRequest, NotFound

api = Namespace('auth', description='User Auth')

from .views import *


@api.errorhandler(BadRequest)
def login_error_handler(error: BadRequest):
    # logger.error('error on login| %s', error.description)
    return {'status': 'error',
            'message': error.description,
            'error_name': error.name,
            }, error.code


@api.errorhandler(NotFound)
def login_error_handler(error: BadRequest):
    logger.error('error on login| %s', error.description)
    return {'status': 'error',
            'message': error.description,
            'error_name': error.name,
            }, error.code


@api.errorhandler(LoginError)
def login_error_handler(error: LoginError):
    # logger.error('error on login| %s', error.description)
    return {'status': 'error',
            'message': error.description,
            'error_name': error.name,
            'error_code': error.errcode
            }, HTTPStatus.UNAUTHORIZED
