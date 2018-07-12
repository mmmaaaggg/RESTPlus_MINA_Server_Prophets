#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/3/30 17:43
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from flask_restplus import Namespace, Resource, fields, reqparse

api = Namespace('forecast', description='预测')

from .views import *
