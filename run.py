#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/6 10:21
@File    : run.py.py
@contact : mmmaaaggg@163.com
@desc    : 
"""

if __name__ == '__main__':
    from app.app import app

    app.run(debug=True)
