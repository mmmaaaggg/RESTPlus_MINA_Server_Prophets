#!/usr/bin/env python
# coding=utf-8
from app import db, login_manager
from flask import current_app, url_for, session
from flask_login import UserMixin, AnonymousUserMixin
from sqlalchemy.exc import IntegrityError
from datetime import datetime
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
import logging
logger = logging.getLogger()


class User(UserMixin, db.Model):
    """
    class User --> table users
    同时支持会微信用户、注册登陆用户
    """
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    login_wx = db.Column(db.SmallInteger, default=0, server_default='0')  # 允许微信用户身份登陆
    login_pwd = db.Column(db.SmallInteger, default=0, server_default='0')  # 允许注册用户身份登陆
    # 微信用户信息
    got_auth = db.Column(db.Boolean(), nullable=False, server_default='0')  # 是否已经取得授权
    openId = db.Column(db.String(50), unique=True)
    username = db.Column(db.String(100), default='unnamed', nullable=False)
    gender = db.Column(db.Integer)
    city = db.Column(db.String(40))
    province = db.Column(db.String(40))
    country = db.Column(db.String(40))
    avatarUrl = db.Column(db.String(200))
    cTimestamp = db.Column(db.DateTime, default=datetime.now())
    # 注册用户信息
    password = db.Column(db.String(255), default='', nullable=False, server_default='')
    # User email information
    email = db.Column(db.String(255), unique=True)
    confirmed_at = db.Column(db.DateTime())
    # User information
    active = db.Column('is_active', db.Boolean(), default=0, nullable=False, server_default='0')

    def __repr__(self):
        return '<User(%d) %r>' % (self.id, self.username)

    # 序列化转换: 资源->JSON
    def to_json(self):
        json_user = {
            'uri': url_for('api.get_user', id=self.id, _external=True),
            'openId': self.openId,
            'username': self.username,
            'gender': self.gender,
            'city': self.city,
            'province': self.province,
            'country': self.country,
            'avatarUrl': self.avatarUrl,
            # 'cashbox': str(self.cashbox),
            'cTimestamp': self.cTimestamp.strftime('%Y-%m-%d')
        }
        return json_user

    # 序列化转换：JSON->资源
    @staticmethod
    def from_json(json_user):
        openId = json_user.get('openId')
        username = json_user.get('username')
        gender = json_user.get('gender')
        city = json_user.get('city')
        province = json_user.get('province')
        country = json_user.get('country')
        avatarUrl = json_user.get('avatarUrl')
        # cashbox = Decimal(json_user.get('cashbox'))
        #    if body is None or body = '':
        #      raise ValidationError('user does not hava a name')
        return User(openId=openId, username=username, gender=gender, city=city, province=province, country=country,
                    avatarUrl=avatarUrl,
                    # cashbox=cashbox
                    )

    # 生成初始数据
    @staticmethod
    def generate_users():
        user = User(id=1, login_wx=1, openId='oAk3s0Bef6kcKKf0waVJvDUlrShE', username=u'飘移', gender=1, city='Jinan',
                    province='Shandong', country='CN',
                    avatarUrl='http://wx.qlogo.cn/mmopen/vi_32/Q0j4TwGTfTL04eZJ57hiaQcuWk4kT5vvY6Epmmo6smJ94ejJqWZrbTIriaftjBhvDfeIwsxBTM5hibpXx3CiaC9T0Q/0',
                    # cashbox='0.0'
                    )
        db.session.add(user)
        try:
            db.session.commit()
            logger.debug('generate users successfully')
        except IntegrityError:
            db.session.rollback()
            logger.debug('fail to generate users')

    # 生成授权token
    def generate_auth_token(self, expiration=3600):
        s = Serializer(current_app.config['SECRET_KEY'], expires_in=expiration)
        return s.dumps({'openId': self.openId}).decode()

    # 验证授权token
    @staticmethod
    def verify_auth_token(token):
        s = Serializer(current_app.config['SECRET_KEY'])
        try:
            data = s.loads(token.encode())
            logger.debug(data)
        except:
            return None
        return User.query.filter_by(openId=data['openId']).first()


@login_manager.user_loader
def load_user(user_id):
    """user_loader回调，用于从会话中存储的用户ID重新加载用户对象"""
    logger.debug(user_id)
    return User.query.get(int(user_id))


@login_manager.request_loader
def load_user_from_request(request):
    token = request.headers.get('token')
    if token is not None and token in current_app.login_user_dic:
        curr_user = current_app.login_user_dic[token]
        user_id = curr_user.id
        session['user_id'] = user_id
        logger.debug("curr_user: %d %s", curr_user.id, curr_user)
        return curr_user


class AnonymousUser(AnonymousUserMixin):
    """class AnonymousUser --> no table """
    def can(self, permissions):
        return False

    def is_administrator(self):
        return False


login_manager.anonymous_user = AnonymousUser
