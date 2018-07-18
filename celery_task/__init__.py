#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/4/2 9:55
@File    : __init__.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from celery import Celery


def make_celery(app)->Celery:
    """
    官方文档推荐做法
    http://flask.pocoo.org/docs/0.12/patterns/celery/
    :param app:
    :return:
    """
    celery = Celery(app.import_name,
                    backend=app.config['CELERY_RESULT_BACKEND'],
                    broker=app.config['CELERY_BROKER_URL'],
                    include=['celery_task.task'],
                    )
    celery.conf.update(app.config)
    task_base = celery.Task

    class ContextTask(task_base):
        abstract = True

        def __call__(self, *args, **kwargs):
            with app.app_context():
                return task_base.__call__(self, *args, **kwargs)
    celery.Task = ContextTask
    return celery
