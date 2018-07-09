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

api = Namespace('auth', description='User Auth')

from .views import *
