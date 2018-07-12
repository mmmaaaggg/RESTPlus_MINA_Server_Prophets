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
    import logging
    from app.config import config
    from app.app import app
    logger = logging.getLogger()
    if config.APP_ENABLE_SSL:
        logger.info('ssl path: %s', config.HTTPS_SSL_PEM_FILE_PATH)

    app.run(
        host='0.0.0.0', port=config.APP_PORT, debug=True,
        ssl_context='adhoc',
        # ssl_context=(config.HTTPS_SSL_PEM_FILE_PATH, config.HTTPS_SSL_KEY_FILE_PATH) if config.APP_ENABLE_SSL else None
    )
