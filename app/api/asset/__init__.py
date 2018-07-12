#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/5 15:26
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import logging
from flask_restplus import Namespace

logger = logging.getLogger()
# __name__.split('.')[-1] 相当于 asset 文件名
# 目标文件默认使用 templates/asset 下的文件
# file_name = __name__.split('.')[-1]
logger.info('import %s', __name__)
api = Namespace('asset', description='资产列表')

from .views import *
