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

# from .cat import api as ns1
from .auth.views import api as ns2

api = Api(
    title='MINA Prophets Server',
    version='1.0',
    description='预言家小程序服务器端API',
)

# api.add_namespace(ns1)
api.add_namespace(ns2)
