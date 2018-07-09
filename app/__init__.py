#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/4 15:44
@File    : app.py
@contact : mmmaaaggg@163.com
@desc    :
"""
from flask_login import LoginManager
from flask_sqlalchemy import SQLAlchemy
# 登录模块
login_manager = LoginManager()
# Initialize Flask-SQLAlchemy
db = SQLAlchemy()
