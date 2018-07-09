#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/5/23 9:23
@File    : make_ssl_key.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import os
from werkzeug import serving


def create_crt(path, host):
    serving.make_ssl_devcert(path, host)


if __name__ == "__main__":
    path = os.environ.get('FLASK_KEY_PATH', os.path.abspath(os.curdir))
    host = os.environ.get('FLASK_HOSTNAME', 'localhost')

    if not os.path.exists(os.path.join(path, 'ssl.key')):
        file_prefix = os.path.join(path, 'ssl')
        create_crt(path, host)
    else:
        print('ssl keys already exists')
