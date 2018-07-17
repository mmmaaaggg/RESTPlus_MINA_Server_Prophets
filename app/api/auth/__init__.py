#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/6 13:44
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from flask_restplus import Namespace
import logging
logger = logging.getLogger(__name__)
# __name__.split('.')[-1] 相当于 asset 文件名
# 目标文件默认使用 templates/asset 下的文件
# file_name = __name__.split('.')[-1]
logger.info('import %s', __name__)
api = Namespace('auth', description='User Auth')

from .views import *


# 异常处理可以在 总的 __init__.py文件中统一处理，也可以再每一个 Model 的 __init__.py 文件中处理
# @api.errorhandler(LoginError)
# def login_error_handler(error: LoginError):
#     # logger.error('error on login| %s', error.description)
#     return {'status': 'error',
#             'message': error.description,
#             'error_name': error.name,
#             'error_code': error.errcode
#             }, HTTPStatus.UNAUTHORIZED
