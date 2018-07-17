#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/4 15:43
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""

from flask_restplus import Api
from flask_restplus._http import HTTPStatus
from app.api.exceptions import LoginError
from werkzeug.exceptions import BadRequest, NotFound
import logging
from .auth import api as ns2
from .forecast import api as ns3
from .asset import api as ns4
logger = logging.getLogger()
api = Api(
    title='MINA Prophets Server',
    version='1.0',
    description='预言家小程序服务器端API',
)

# api.add_namespace(ns1)
api.add_namespace(ns2)
api.add_namespace(ns3)
api.add_namespace(ns4)


@api.errorhandler(Exception)
def login_error_handler(error: Exception):
    """仅作为一个异常处理的例子"""
    # logger.error('error on login| %s', error.description)
    return {'status': 'error',
            'message': error.args[0],
            'error_name': error.__class__.__name__,
            }, HTTPStatus.BAD_REQUEST


@api.errorhandler(BadRequest)
def login_error_handler(error: BadRequest):
    # logger.error('error on login| %s', error.description)
    return {'status': 'error',
            'message': error.description,
            'error_name': error.name,
            }, HTTPStatus.BAD_REQUEST


@api.errorhandler(NotFound)
def login_error_handler(error: BadRequest):
    logger.error('error on login| %s', error.description)
    return {'status': 'error',
            'message': error.description,
            'error_name': error.name,
            }, HTTPStatus.NOT_FOUND


@api.errorhandler(LoginError)
def login_error_handler(error: LoginError):
    # logger.error('error on login| %s', error.description)
    return {'status': 'error',
            'message': error.description,
            'error_name': error.name,
            'error_code': error.errcode
            }, HTTPStatus.UNAUTHORIZED
