#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/7/6 10:39
@File    : auth.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
import requests
import logging
# 验签数据
import hashlib
# 解密加密数据，获取watermark中的appid
import base64
import json
from Crypto.Cipher import AES
# Flask Module
from flask import request, current_app, jsonify, session
from flask_login import login_required, login_user, logout_user, current_user
from flask_restplus import Namespace, Resource, fields, reqparse
# App Module
from app.config import config
from app import db
from app.api.auth import api
from app.api.auth.models import User
logger = logging.getLogger()

login_model = api.model('Login', {
    'userid': fields.String(required=True, description='The user identifier'),
    'is_first': fields.Boolean(required=True, description="It's a new user"),
    'got_auth': fields.Boolean(required=True, description="It's a auth user"),
    'token': fields.String(required=True, description='User Token'),
    'openid': fields.String(required=True, description='User OpenID'),
    'expired': fields.String(required=True, description='Seconds of Expired'),
})

login_req_parser = reqparse.RequestParser()
login_req_parser.add_argument('code', type=str, required=True, help='wx.login() 返回 code')
login_req_parser.add_argument('encryptedData', type=str, help='解密加密数据，校验appid')
login_req_parser.add_argument('iv', type=str, help='解密加密数据，校验appid')
login_req_parser.add_argument('rawData', type=str, help='校验签名，判别数据完整性')
login_req_parser.add_argument('signature', type=str, help='校验签名，判别数据完整性')

login_token_req_parser = reqparse.RequestParser()
login_token_req_parser.add_argument('token', type=str, location='headers', required=True, help='登录 token')


def sha1Sign(session_key, rawData):
    logger.debug(rawData.encode('utf-8'))
    data = '%s%s' % (rawData.encode('utf8'), session_key)
    return hashlib.sha1(str(data)).hexdigest()


def decrypt(session_key, encryptedData, iv):
    sessionKey = base64.b64decode(session_key)
    encryptedData = base64.b64decode(encryptedData)
    iv = base64.b64decode(iv)

    cipher = AES.new(sessionKey, AES.MODE_CBC, iv)
    s = cipher.decrypt(encryptedData)
    decrypted = json.loads(s[:-ord(s[len(s) - 1:])])
    logger.debug(decrypted)

    return decrypted['watermark']['appid']


@api.route('/login')
class Login(Resource):
    """
    登录API
    """

    @api.doc('user login')
    @api.expect(login_req_parser)
    @api.marshal_list_with(login_model)
    def get(self):
        """
    微信小程序注册登陆接口
    :return:
    """

        args = login_req_parser.parse_args()
        code = args['code']
        encryptedData = args['encryptedData'] if 'encryptedData' in args else None
        rawData = args['rawData'] if 'rawData' in args else None
        signature = args['signature'] if 'signature' in args else None
        iv = args['iv'] if 'iv' in args else None
        logger.debug("接受登陆请求 args: %s", args)
        # logger.debug("接受登陆请求 data: %s", request.data)
        # logger.debug("接受登陆请求 form: %s", request.form)
        # logger.debug("接受登陆请求 json: %s", request.json)
        # 用js_code，appid，secret，grant_type向微信服务器获取session_key,openid,expires_in
        data = dict()
        data['appid'] = current_app.config['APP_ID']
        data['secret'] = current_app.config['SECRET_KEY']
        data['js_code'] = code
        data['grant_type'] = 'authorization_code'
        logger.debug('授权登陆提交信息: %s', data)
        res = requests.get('https://api.weixin.qq.com/sns/jscode2session', params=data).json()
        logger.debug('微信API返回信息: %s', res)
        if 'session_key' in res:
            session_key = res['session_key']
            openid = res['openid']
            # expires_in = res['expires_in']

            # # 校验签名，判别数据完整性
            if rawData is not None:
                if sha1Sign(session_key, rawData) != signature:
                    raise ValueError('Invalid rawData!')

            # 解密加密数据，校验appid
            if encryptedData is not None:
                if decrypt(session_key, encryptedData, iv) != data['appid']:
                    raise ValueError('Invalid encryptedData!')

            # 默认是老客户
            is_first = False

            # 根据openid是否插入用户
            user = User.query.filter_by(openId=openid).first()
            if user is None:
                # logger.debug('add user: %s' % rawData)
                is_first = True
                # rData = json.loads(rawData)
                user = User(openId=openid,
                            login_wx=1,
                            # gender=rData['gender'],
                            # city=rData['city'],
                            # province=rData['province'],
                            # country=rData['country'],
                            # avatarUrl=rData['avatarUrl'],
                            )
                db.session.add(user)
                db.session.commit()
                logger.info("新用户登陆")
            else:
                logger.info("已注册用户登陆")

            # 登录用户，并返回由openid和SECRET_KEY构成的token
            login_user(user, True)
            expired = config.SESSION_TIMEOUT_SECONDS
            token = user.generate_auth_token(expiration=expired)
            current_app.login_user_dic[token] = user
            logger.debug('token: %s' % token)
            return {'userid': user.id,
                    'is_first': is_first,
                    'got_auth': user.got_auth,
                    'token': token,
                    'openid': user.openId,
                    'expired': expired
                    }
        return jsonify(res)


@api.route('/login_test')
class LoginTest(Resource):
    """
    登录测试
    """
    @login_required
    @api.expect(login_token_req_parser)
    def get(self):
        args = login_token_req_parser.parse_args()
        token = args['token']
        logger.debug("session data:")
        for num, (key, val) in enumerate(session.items()):
            logger.debug("%2d) %s = %s", num, key, val)
        logger.debug("%s", type(current_user))
        return jsonify({'status': 'ok', 'message': 'Hello world!\nYou have login',
                        'token': token,
                        'current_user': str(current_user)})


@api.route('/login_force/<int:user_id>')
@api.param('user_id', '强制以制定id用户身份登录')
class LoginForce(Resource):
    @api.doc('user login')
    @api.marshal_list_with(login_model)
    def get(self, user_id=1):
        """
        仅供测试使用。默认第一个微信用户身份进行登陆。
        TODO: 该部分代码上线后需要禁用
        :return:
        """
        user = User.query.filter(User.id == user_id).first()
        if user is None:
            return jsonify({'errmsg': 'missing code, hints: [ req_id: Nlh0Ga0255th54 ]', 'errcode': 41008})

        is_first = False
        # 登录用户，并返回由openid和SECRET_KEY构成的token
        login_user(user, True)
        expired = config.SESSION_TIMEOUT_SECONDS
        token = user.generate_auth_token(expiration=expired)
        current_app.login_user_dic[token] = user
        logger.debug('token: %s' % token)
        return {'user_id': user.id,
                'is_first': is_first,
                'got_auth': user.got_auth,
                'token': token,
                'openid': user.openId,
                'expired': expired
                }
