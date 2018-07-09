#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/3/30 17:53
@File    : config.py
@contact : mmmaaaggg@163.com
@desc    :
"""
import os
import logging
from logging.config import dictConfig
import platform
# IS_LINUX_OS = platform.os.name != 'nt'
# if IS_LINUX_OS:
#     from celery.schedules import crontab

basedir = os.path.abspath(os.path.dirname(__file__))


# Use a Class-based config to avoid needing a 2nd file
# os.getenv() enables configuration through OS environment variables
class ConfigClass(object):
    # Flask settings
    SECRET_KEY = os.environ.get('SECRET_KEY') or '***'
    API_KEY = os.environ.get('API_KEY') or '***'
    APP_ID = '***'
    MCH_ID = '***'
    # UPLOAD_FOLDER = '/home/ubuntu/SweetHeart/imgs/upload'
    ALLOWED_EXTENSIONS = set(['txt', 'pdf', 'png', 'jpg', 'JPG', 'jpeg', 'gif'])

    # Flask Sql Alchemy settings
    BIND_DB_NAME_MD = 'db_md'
    # SQLALCHEMY_DATABASE_URI = os.getenv('DATABASE_URL',     'sqlite:///basic1_app.sqlite')
    SQLALCHEMY_DATABASE_URI = 'mysql://mg:***@10.0.3.66/prophet'
    SQLALCHEMY_BINDS = {
        BIND_DB_NAME_MD: 'mysql://mg:***@10.0.3.66/fof_ams_dev'
    }
    CSRF_ENABLED = True

    # Flask-Mail settings
    USER_SEND_PASSWORD_CHANGED_EMAIL = False
    USER_SEND_REGISTERED_EMAIL = False
    USER_SEND_USERNAME_CHANGED_EMAIL = False
    MAIL_USERNAME = os.getenv('MAIL_USERNAME', '265590706@qq.com')
    MAIL_PASSWORD = os.getenv('MAIL_PASSWORD', '***')
    MAIL_DEFAULT_SENDER = os.getenv('MAIL_DEFAULT_SENDER', '"MyApp" <265590706@qq.com>')
    MAIL_SERVER = os.getenv('MAIL_SERVER', 'smtp.qq.com')
    MAIL_PORT = int(os.getenv('MAIL_PORT', '465'))
    MAIL_USE_SSL = int(os.getenv('MAIL_USE_SSL', True))

    # Flask-User settings
    USER_APP_NAME = "AppName"  # Used by email templates

    # Celery settings
    CELERY_BROKER_URL = 'redis://127.0.0.1:6379',
    CELERY_RESULT_BACKEND = 'redis://127.0.0.1:6379'
    # CELERY_ACCEPT_CONTENT = ['json', 'pickle']
    CELERY_TIMEZONE = 'Asia/Shanghai'
    # CELERYBEAT_SCHEDULE = {
    #     'ptask': {
    #         'task': 'bg_tasks.task.chain_task',
    #         # 'schedule': timedelta(seconds=5),
    #         'schedule': crontab(hour='18') if IS_LINUX_OS else None,
    #         # 'args': (16, 73),  # 当前任务没有参数
    #     },
    # }

    # SSL key and pem
    HTTPS_SSL_KEY_FILE_PATH = os.path.join(os.path.split(os.path.realpath(__file__))[0], '..',
                                           'ca', r'1528061526323.key')
    HTTPS_SSL_PEM_FILE_PATH = os.path.join(os.path.split(os.path.realpath(__file__))[0], '..',
                                           'ca', r'1528061526323.pem')

    # 开启 HTTPS 服务
    APP_ENABLE_SSL = False
    # APP_PORT
    APP_PORT = 8100

    # 微信用户 session 超时信息
    SESSION_TIMEOUT_SECONDS = 2592000  # 2400 * 24 * 30

    # 分页显示，每页的数量
    APP_PAGINATE_ITEMS_COUNT = 5

    # log settings
    logging_config = dict(
        version=1,
        formatters={
            'simple': {
                'format': '%(asctime)s %(name)s|%(module)s.%(funcName)s:%(lineno)d %(levelname)s %(message)s'}
        },
        handlers={
            'file_handler':
                {
                    'class': 'logging.handlers.RotatingFileHandler',
                    'filename': 'logger.log',
                    'maxBytes': 1024 * 1024 * 10,
                    'backupCount': 5,
                    'level': 'DEBUG',
                    'formatter': 'simple',
                    'encoding': 'utf8'
                },
            'console_handler':
                {
                    'class': 'logging.StreamHandler',
                    'level': 'DEBUG',
                    'formatter': 'simple'
                }
        },

        root={
            'handlers': ['console_handler', 'file_handler'],
            'level': logging.DEBUG,
        }
    )
    logging.getLogger('sqlalchemy.engine').setLevel(logging.WARN)
    dictConfig(logging_config)




class LocalConfig(ConfigClass):

    # 开启 HTTPS 服务
    APP_ENABLE_SSL = True
    # APP_PORT
    APP_PORT = 443


config = ConfigClass()
