#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/4 15:44
@File    : app.py
@contact : mmmaaaggg@163.com
@desc    :
"""
from flask import Flask
from app.config import config
from app.api import api
from app import db, login_manager

app = Flask(__name__)
app.debug = True
# app.config.from_object(__name__+'.ConfigClass')
app.config.from_object(config)
api.init_app(app)

# reference : http://flask-sqlalchemy.pocoo.org/2.3/contexts/
# app.app_context().push()
ctx = app.app_context()
ctx.push()

# 登录模块
# login_manager = LoginManager(app)
# 将扩展对象绑定到应用上
login_manager.init_app(app)

# Initialize Flask extensions
# db = SQLAlchemy(app)                            # Initialize Flask-SQLAlchemy
# Create all database tables
db.init_app(app)
db.app = app
# db.create_all()

# Setup Flask-User
# db_adapter = SQLAlchemyAdapter(db, User)  # Register the User model
# user_manager = UserManager(db_adapter, app)  # Initialize Flask-User
# login_user_dic 用于存储当前以及登陆的用户对象信息，小程序用户登陆验证采用 request_loader 方式，需要自定义验证
app.login_user_dic = {}


if __name__ == '__main__':
    app.run(debug=True)
