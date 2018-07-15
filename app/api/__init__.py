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
from app.api.exceptions import LoginError
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
