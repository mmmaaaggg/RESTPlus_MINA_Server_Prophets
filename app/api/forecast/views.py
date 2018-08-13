#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/3/30 17:47
@File    : forecast.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from datetime import timedelta, date
from flask_restplus import Resource, fields, reqparse
from flask_restplus._http import HTTPStatus
from werkzeug.exceptions import BadRequest
from app.api.forecast import api, logger
from app import db
from app.config import config
from flask import request, session
from flask_login import login_required
import json
from app.api.auth.models import User
from app.api.forecast.models import PortfolioInfo, PortfolioData, PortfolioValueDaily, \
    PortfolioCompareResult, PortfolioCompareInfo, FavoriteCompare, FavoritePortfolio
from app.api.asset.views import get_asset_name
from app.utils.fh_utils import date_2_str, populate_obj, calc_performance, datetime_2_str
import pandas as pd
import numpy as np
from sqlalchemy import func, or_, and_, column, not_

error_model = api.model('error', {
    'status': fields.String(description='状态', default='error'),
    'message': fields.String(description='错误信息'),
})

error_action_model = api.model('error', {
    'status': fields.String(description='状态', default='error'),
    'id': fields.Integer(description='操作对象ID'),
    'message': fields.String(description='错误信息'),
})

# 相关 parser 文件
paginate_model = api.model('paginate_model', {
    'page': fields.Integer(required=True, description='当前页码'),
    'pages': fields.Integer(required=True, description='总共页数'),
    'count': fields.Integer(required=True, description='当前记录数'),
    'total': fields.Integer(required=True, description='总共记录数'),
    'has_prev': fields.Boolean(required=True, description='有前一页'),
    'has_next': fields.Boolean(required=True, description='有后一页'),
    'data': fields.List(fields.Raw),
})

login_parser = reqparse.RequestParser().add_argument(
    'token', type=str, location='headers', required=True, help='登录 token'
)

paginate_parser = reqparse.RequestParser().add_argument(
    'token', type=str, location='headers', required=True, help='登录 token'
).add_argument(
    'page_no', type=int, default=1, help='请求页码'
).add_argument(
    'count', type=int, default=config.APP_PAGINATE_ITEMS_COUNT, help='每页数量'
)

count_parser = reqparse.RequestParser().add_argument(
    'token', type=str, location='headers', required=True, help='登录 token'
).add_argument(
    'count', type=int, default=config.APP_PAGINATE_ITEMS_COUNT, help='查询记录数，仅在状态为recent时有效'
)

data_matrix_model = api.model('asset_candle_result_model', {
    'data': fields.List(fields.List(fields.Raw)),
    'count': fields.Integer(description='记录数'),
})

data_result_model = api.model('asset_candle_result_model', {
    'data': fields.List(fields.Raw),
    'count': fields.Integer(description='记录数'),
})

pl_asset_weight_model = api.model('pl_asset_weight_model', {
    'asset_code': fields.String(description='资产代码', required=True),
    'asset_type': fields.String(description='资产类型', required=True),
    'weight': fields.Float(description='权重', required=True),
    'direction': fields.Integer(description='方向 1 做多 -1 做空', required=True),
})

pl_data_model = api.model('portfolio_data_model', {
    'trade_date': fields.Date(description='交易日期', required=True),
    'price_type': fields.String(description='价格基准 open 开盘价、close 收盘价', required=True),
    'data': fields.List(fields.Nested(pl_asset_weight_model), description='投资组合'),
})

pl_create_model = api.model('portfolio_create_model', {
    'name': fields.String(description='名称', required=True),
    'access_type': fields.String(description='public 公开 private 私有', required=True),
    'desc': fields.String(description='描述'),
    'pl_data': fields.Nested(pl_data_model, description='投资组合信息'),
})

cmp_params_model = api.model('cmp_params_model', {
    "asset_type_1": fields.String(description='资产类别：index stock fund future portfolio'),
    "asset_type_2": fields.String(description='资产类别：index stock fund future portfolio value'),
    "asset_type_3": fields.String(description='资产类别：index stock fund future portfolio value', required=False),
    "asset_1": fields.String(description='资产代码（例如："000300.SH"）或投资组合ID', required=False),
    "value_1": fields.Float(description='数值', required=False),
    "asset_2": fields.String(description='资产代码（例如："000300.SH"）或投资组合ID', required=False),
    "value_2": fields.Float(description='数值', required=False),
    "asset_3": fields.String(description='资产代码（例如："000300.SH"）或投资组合ID', required=False),
    "value_3": fields.Float(description='数值', required=False),
    "compare_type": fields.String(
        description='比较方式 相对收益率 "rel.rr", 绝对收益率 "abs.rr", 绝对点位"abs.fix_point"'),
    "compare_method": fields.String(description="比较符 '>', '<', 'between'"),
    "date_start": fields.Date(description='起始计算日期，仅用于对收益率比较时使用', required=False),
})

cmp_create_model = api.model('portfolio_create_model', {
    'name': fields.String(description='名称', required=True),
    'access_type': fields.String(description='public 公开 private 私有', required=True),
    'date_from': fields.Date(description='起始日期', required=False),
    'date_to': fields.Date(description='起始日期', required=False),
    'desc': fields.String(description='描述信息', required=True),
    'params': fields.Nested(cmp_params_model, description='投资组合信息'),
})

create_rsp_model = api.model('create_rsp_model', {
    'status': fields.String(descrption='状态'),
    'message': fields.String(description='备注', required=False),
    'id': fields.Integer(description='新增对象ID'),
})

delete_rsp_model = api.model('delete_rsp_model', {
    'status': fields.String(descrption='状态'),
    'id': fields.Integer(description='新增对象ID'),
})

summary_item_model = api.model('summary_item_model', {
    'name': fields.String('名称'),
    'status': fields.String('状态'),
    'count': fields.String('数量'),
})

summary_model = api.model('summary_model', {
    'data': fields.List(fields.Nested(summary_item_model))
})

stats_model = api.model('stats_model', {
    "general": fields.Raw,
    "performance": fields.Raw,
    "others": fields.Raw,
})


@api.route('/cmp')
class CompareInfoResource(Resource):

    @api.doc('创建预测信息')
    @api.expect(login_parser, cmp_create_model)
    @api.marshal_with(create_rsp_model)
    @login_required
    @api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
    def post(self):
        """
        创建预测信息
        """
        # logger.debug("request.args: %s", request.args)
        # logger.debug("request.values: %s", request.values)
        # logger.debug("request.json: %s", request.json)
        # logger.debug("request.data: %s", request.data)
        data_dic = request.get_json() or request.form
        logger.debug("data_dic: %s", data_dic)

        # 添加投资组合信息
        data_obj = PortfolioCompareInfo()
        populate_obj(data_obj, data_dic,
                     attr_list=["name", "date_from", "date_to", "access_type", "desc"],
                     error_if_no_key=True)
        # logger.debug("data_dic['params']<%s>: %s", type(data_dic['params']), data_dic['params'])
        data_obj.params = json.dumps(data_dic['params'])
        # TODO: 需要进行：1）参数合法性检查 2）pl_id user_id create_dt 等参数不得传入，类似无效参数过滤
        user_id = session.get('user_id')
        data_obj.create_user_id = user_id
        db.session.add(data_obj)
        db.session.commit()
        logger.info('%s：id=%d 成功插入数据库 %s', data_obj.__class__.__name__, data_obj.cmp_id, data_obj.__tablename__)

        return {"status": "ok", 'id': data_obj.cmp_id}


@api.route('/cmp/summary')
class CompareSummaryResource(Resource):

    @api.doc('获取预测比较汇总信息')
    @api.expect(login_parser)
    @api.marshal_with(summary_model)
    @login_required
    def get(self):
        """
        获取预测比较汇总信息：待验证，已验证，关注预言 数量及对应 status
        """
        user_id = session.get('user_id')
        sql_str = """SELECT sum(if(trade_date_max IS NULL, 0, if(trade_date_max<cmp_info.date_to, 1, 0))) unverified,
                sum(if(trade_date_max IS NULL, 0, if(trade_date_max>=cmp_info.date_to, 1, 0))) verified
                FROM 
                pl_compare_info cmp_info
                LEFT JOIN
                (
                  SELECT cmp_id, max(trade_date) trade_date_max FROM pl_compare_result GROUP BY cmp_id
                ) cmp_rslt
                ON cmp_info.cmp_id = cmp_rslt.cmp_id
                WHERE is_del=0"""
        raw = db.engine.execute(sql_str).first()
        count_unverified, count_verified = raw

        favorite = db.session.query(func.count()).filter(FavoriteCompare.user_id == user_id).scalar()

        ret_data = {
            'data': [
                {
                    "name": "待验证",
                    "status": "unverified",
                    "count": float(count_unverified)
                },
                {
                    "name": "已验证",
                    "status": "verified",
                    "count": float(count_verified)
                },
                {
                    "name": "关注预言",
                    "status": "favorite",
                    "count": float(favorite)
                }
            ]
        }
        return ret_data


@api.route('/cmp/<string:status>')
@api.param('status', 'my all star verified unverified 其中之一')
@api.response(HTTPStatus.BAD_REQUEST, "item_order 参数错误", model=error_model)
class CompareInfoWithStatusResource(Resource):

    @api.doc('获取比较列表数据（分页）')
    @api.expect(paginate_parser)
    @api.marshal_with(paginate_model)
    @login_required
    @api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
    def get(self, status):
        """
        获取比较列表数据（分页）
        """
        args = paginate_parser.parse_args()
        page_no = args['page_no']
        count = args['count']
        user_id = session.get('user_id')
        logger.debug('get_cmp_list user_id:%s', user_id)
        if status == 'my':
            filter_c = PortfolioCompareInfo.create_user_id == user_id
            having_c = None
        elif status == 'all':
            filter_c = or_(PortfolioCompareInfo.create_user_id == user_id, PortfolioCompareInfo.access_type == 'public')
            having_c = None
        elif status == 'star':
            filter_c = and_(
                or_(PortfolioCompareInfo.create_user_id == user_id, PortfolioCompareInfo.access_type == 'public'),
                not_(func.isnull(FavoriteCompare.update_time))
            )
            having_c = None
        elif status == 'verified':
            filter_c = or_(PortfolioCompareInfo.create_user_id == user_id, PortfolioCompareInfo.access_type == 'public')
            having_c = column('complete_rate') >= 1
        elif status == 'unverified':
            filter_c = or_(PortfolioCompareInfo.create_user_id == user_id, PortfolioCompareInfo.access_type == 'public')
            having_c = or_(column('complete_rate').is_(None), column('complete_rate') < 1)
        else:
            raise KeyError('status 参数错误 status = %s' % status)
        # 整理数据
        # logger.debug("data_list_df len:%d", data_list_df.shape[0])
        # data_list_df = data_list_df.where(data_list_df.notna(), None)
        # data_list = data_list_df.to_dict('record')
        # data_table_dic = {'data': data_list}
        # logger.debug(data_table_dic)

        query = PortfolioCompareInfo.query.outerjoin(
            PortfolioCompareResult
        ).group_by(PortfolioCompareResult.cmp_id).add_columns(
            func.count().label('tot_count'),
            func.min(PortfolioCompareResult.trade_date).label('trade_date_min'),
            func.max(PortfolioCompareResult.trade_date).label('trade_date_max'),
            func.sum(PortfolioCompareResult.result).label('fit_count'),
            (func.sum(PortfolioCompareResult.result) / func.count()).label('fit_rate'),
            (
                    (func.max(PortfolioCompareResult.trade_date) - PortfolioCompareInfo.date_from) /
                    (PortfolioCompareInfo.date_to - PortfolioCompareInfo.date_from)
            ).label('complete_rate')
        ).outerjoin(User).add_columns(
            User.username
        ).outerjoin(
            FavoriteCompare,
            and_(PortfolioCompareInfo.cmp_id == FavoriteCompare.cmp_id, FavoriteCompare.user_id == user_id)
        ).add_columns(
            func.if_(func.isnull(FavoriteCompare.update_time), 0, 1).label('favorite')
        ).filter(
            and_(
                filter_c,
                PortfolioCompareInfo.is_del == 0)
        )
        if having_c is None:
            pagination = query.paginate(page_no, count)
        else:
            pagination = query.having(having_c).paginate(page_no, count)

        logger.debug('%d / %d 页  %d / %d 条数据',
                     pagination.page, pagination.pages, len(pagination.items), pagination.total)
        ret_dic_list = [{
            'cmp_id': data.PortfolioCompareInfo.cmp_id,
            'name': data.PortfolioCompareInfo.name,
            'status': data.PortfolioCompareInfo.status,
            'params': data.PortfolioCompareInfo.params,
            'desc': data.PortfolioCompareInfo.desc,
            'date_from': date_2_str(data.PortfolioCompareInfo.date_from),
            'date_to': date_2_str(data.PortfolioCompareInfo.date_to),
            'trade_date_min': date_2_str(data.trade_date_min),
            'trade_date_max': date_2_str(data.trade_date_max),
            'create_user_id': data.PortfolioCompareInfo.create_user_id,
            'username': data.username,
            'favorite': data.favorite,
            'complete_rate': None if data.complete_rate is None else float(data.complete_rate),
        } for data in pagination.items]
        ret_dic = {
            'page': pagination.page,
            'pages': pagination.pages,
            'count': len(pagination.items),
            'total': pagination.total,
            'has_prev': pagination.has_prev,
            'has_next': pagination.has_next,
            'data': ret_dic_list,
        }
        return ret_dic


@api.route('/cmp/rst/<int:_id>')
@api.param('_id', 'ID')
@api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
class CompareResultResource(Resource):
    """
    预测结果数据（供 charts 使用，无需分页返回）
    """

    @api.doc('预测结果数据（供 charts 使用，无需分页返回）')
    @api.expect(login_parser)
    @api.marshal_with(data_matrix_model)
    @login_required
    def get(self, _id):
        """
        预测结果数据（供 charts 使用，无需分页返回）
        """
        sql_str = """SELECT DATE_FORMAT(trade_date, "%%Y-%%m-%%d") trade_date,
        asset_1, asset_2, asset_3, result, shift_value, shift_rate FROM pl_compare_result
        WHERE cmp_id = %s"""
        data_df = pd.read_sql(sql_str, db.engine, params=[_id])
        ret_df = data_df.where(data_df.notna(), None)
        ret_dic_list = ret_df.to_dict('list')
        logger.debug('%d 条数据', ret_df.shape[0])
        ret_dic = {
            'data': ret_dic_list,
            'count': len(ret_dic_list)
        }
        return ret_dic


@api.route('/cmp/favorite/<int:_id>/<int:do>')
class CompareFavorite(Resource):

    @api.doc('添加、修改星标')
    @api.expect(login_parser)
    @api.marshal_with(create_rsp_model)
    @login_required
    @api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
    def post(self, _id: int, do: int):
        """
        添加、修改星标
        """
        user_id = session.get('user_id')
        if do == 0:
            FavoriteCompare.query.filter(FavoriteCompare.cmp_id == _id, FavoriteCompare.user_id == user_id).delete()
            db.session.commit()
            return {'status': 'ok'}
        else:
            data_obj = FavoriteCompare()
            data_obj.cmp_id = _id
            data_obj.user_id = user_id
            db.session.add(data_obj)
            db.session.commit()
            logger.info('%s：id=%d 成功插入数据库 %s', data_obj.__class__.__name__, data_obj.id, data_obj.__tablename__)

            return {"status": "ok", 'id': data_obj.id}


@api.route('/cmp/info/<int:_id>')
@api.param('_id', '预测ID')
class PortfolioInfoActionResource(Resource):

    @api.doc('删除指定预测')
    @api.expect(login_parser)
    @api.marshal_with(delete_rsp_model)
    @login_required
    @api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
    def delete(self, _id):
        """
        删除指定预测
        """
        logger.debug('cmp_id=%d', _id)
        PortfolioCompareInfo.query.filter(PortfolioCompareInfo.cmp_id == _id).update({'is_del': 1})
        db.session.commit()
        return {'status': 'ok', 'id': _id}


@api.route('/pl/summary')
class PortfolioSummaryResource(Resource):

    @api.doc('获取投资组合汇总信息')
    @api.expect(login_parser)
    @api.marshal_with(summary_model)
    @login_required
    def get(self):
        """
        获取投资组合汇总信息：全部组合，我的组合，关注组合 数量及对应 status
        """
        user_id = session.get('user_id')

        count_all, count_my = db.session.query(
            func.count().label('all'),
            func.sum(func.if_(PortfolioInfo.create_user_id == user_id, 1, 0))
        ).filter(PortfolioInfo.is_del == 0).first()

        favorite = db.session.query(func.count()).filter(FavoritePortfolio.user_id == user_id).scalar()

        ret_data = {
            'data': [
                {
                    "name": "全部",
                    "status": "all",
                    "count": float(count_all)
                },
                {
                    "name": "我的",
                    "status": "my",
                    "count": float(count_my)
                },
                {
                    "name": "关注组合",
                    "status": "favorite",
                    "count": float(favorite)
                }
            ]
        }
        return ret_data


@api.route('/pl/data/<int:_id>/<string:status>/<string:method>')
@api.param('_id', 'ID')
@api.param('status', '状态 latest 最近一次调仓数据, recent 最近几日调仓数据，日期逆序排列')
@api.param('method', '以 record 或 date 方式分页显示')
@api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
class PortfolioListResource(Resource):
    """
    获取制定投资组合的成分及权重数据（分页）
    """

    @api.doc('获取制定投资组合的成分及权重数据（分页）')
    @api.expect(paginate_parser)
    @api.marshal_with(paginate_model)
    @login_required
    @api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
    def get(self, _id, status, method):
        """
        获取制定投资组合的成分及权重数据（分页）
        """
        args = paginate_parser.parse_args()
        page_no = args['page_no']
        count = args['count']
        user_id = session.get('user_id')
        logger.debug('get_cmp_list user_id:%s', user_id)
        if method == 'record':
            ret_dic = PortfolioListResource.get_pl_data_list(_id, status, page_no, count)
        elif method == 'date':
            ret_dic = PortfolioListResource.get_pl_data_list_by_date(_id, status, page_no, count)
        else:
            raise KeyError('status 参数错误 method = %s' % method)
        return ret_dic

    @staticmethod
    def get_pl_data_list(_id, status, page_no, count):
        """
        获取制定投资组合的成分及权重数据（分页）
        :param _id:
        :param status:
        :param page_no:
        :param count:
        :return:
        """
        if status == 'latest':
            pagination = PortfolioData.query.filter(
                PortfolioData.pl_id == _id,
                PortfolioData.trade_date == (
                    db.session.query(func.max(PortfolioData.trade_date)).filter(PortfolioData.pl_id == _id)
                )
            ).paginate(page_no, count)
        elif status == 'recent':
            pagination = PortfolioData.query.filter(
                PortfolioData.pl_id == _id).order_by(PortfolioData.trade_date.desc()).paginate(page_no, count)
        else:
            raise KeyError('status = %s 不支持' % status)

        logger.debug('%d / %d 页  %d / %d 条数据',
                     pagination.page, pagination.pages, len(pagination.items), pagination.total)
        date_grouped_dic = {}
        ret_dic_list = []
        for data in pagination.items:
            date_cur = date_2_str(data.trade_date)
            if date_cur in date_grouped_dic:
                data_list = date_grouped_dic[date_cur]
            else:
                data_list = []
                date_grouped_dic[date_cur] = data_list
                ret_dic_list.append({'trade_date': date_cur, 'data': data_list})

            # 获取资产及资产类别中文名称
            asset_name = get_asset_name(data.asset_type, data.asset_code)
            # 因为list是引用性数据，直接放入 ret_dic_list
            # 对 data_list 的修改直接反应到 ret_dic_list 中去
            data_list.append({
                'id': data.id,
                'asset_code': data.asset_code,
                'asset_name': asset_name,
                'asset_type': data.asset_type,
                'trade_date': date_cur,
                'weight': float(data.weight),
                'weight_before': float(data.weight_before),
                'price_type': data.price_type,
                'direction': data.direction,
            })

        ret_dic = {
            'page': pagination.page,
            'pages': pagination.pages,
            'count': len(pagination.items),
            'total': pagination.total,
            'has_prev': pagination.has_prev,
            'has_next': pagination.has_next,
            'data': ret_dic_list,
        }
        # logger.debug(ret_dic)
        return ret_dic

    @staticmethod
    def get_pl_data_list_by_date(_id, status, page_no, count):
        """
        获取制定投资组合的成分及权重数据（分页）
        status
        :param _id:
        :param status: latest 最近一次调仓数据, recent 最近几日调仓数据，日期逆序排列
        :return:
        """
        # logger.info('pl_id=%s, status=%s', _id, status)
        # PortfolioData.query.filter(PortfolioData.id == pl_id, PortfolioData.trade_date == 1)
        if status == 'latest':
            date_cur = db.session.query(func.max(PortfolioData.trade_date)).filter(PortfolioData.pl_id == _id).scalar()
            # 最新调仓日期
            pagination = PortfolioData.query.filter(
                PortfolioData.pl_id == _id,
                PortfolioData.trade_date == date_cur
            ).paginate(page_no, count)
        elif status == 'recent':
            pagination = PortfolioData.query.group_by(PortfolioData.trade_date).filter(
                PortfolioData.pl_id == _id).order_by(PortfolioData.trade_date.desc()).paginate(page_no, count)
        else:
            raise KeyError('status = %s 不支持' % status)

        logger.debug('%d / %d 页  %d / %d 条数据',
                     pagination.page, pagination.pages, len(pagination.items), pagination.total)
        date_list = [data.trade_date for data in pagination.items]

        date_grouped_dic = {}
        ret_dic_list = []
        if len(date_list) > 0:
            # date_grouped_dic 中的 value 为 data_list 实际与 ret_dic_list 中对应日期的 data_list 为同一对象
            # 因此可以直接修改

            items = db.session.query(PortfolioData).filter(
                PortfolioData.pl_id == _id,
                PortfolioData.trade_date.in_(date_list)
            ).all()
            for data in items:
                date_cur = date_2_str(data.trade_date)
                if date_cur in date_grouped_dic:
                    data_list = date_grouped_dic[date_cur]
                else:
                    data_list = []
                    date_grouped_dic[date_cur] = data_list
                    ret_dic_list.append({'trade_date': date_cur, 'data': data_list})

                # 获取资产及资产类别中文名称
                asset_name = get_asset_name(data.asset_type, data.asset_code)
                # 因为list是引用性数据，直接放入 ret_dic_list
                # 对 data_list 的修改直接反应到 ret_dic_list 中去
                data_list.append({
                    'id': data.id,
                    'asset_code': data.asset_code,
                    'asset_name': asset_name,
                    'asset_type': data.asset_type,
                    'trade_date': date_cur,
                    'weight': float(data.weight),
                    'weight_before': float(data.weight_before),
                    'price_type': data.price_type,
                    'direction': data.direction,
                })

        ret_dic = {
            'page': pagination.page,
            'pages': pagination.pages,
            'count': len(pagination.items),
            'total': pagination.total,
            'has_prev': pagination.has_prev,
            'has_next': pagination.has_next,
            'data': ret_dic_list,
        }
        # logger.debug(ret_dic)
        return ret_dic


@api.route('/pl/asset_dist/<int:_id>/<string:status>')
@api.param('_id', 'ID')
@api.param('status', '状态 latest 最近一次调仓数据，recent 最近几日调仓数据，日期逆序排列，all全部历史数据')
class PortfolioAssetDistributionResource(Resource):

    @api.doc('获取制定投资组合的成分及权重数据（分页）')
    @api.expect(count_parser)
    @api.marshal_with(data_result_model)
    @login_required
    @api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
    def get(self, _id, status):
        """
        投资组合资产分布比例
        """
        logger.info('pl_id=%s, status=%s', _id, status)
        if status == 'latest':
            result_list = db.session.query(
                PortfolioData.trade_date, PortfolioData.asset_type, func.sum(PortfolioData.weight).label('weight')
            ).group_by(PortfolioData.trade_date, PortfolioData.asset_type).filter(
                and_(PortfolioData.trade_date == (
                    db.session.query(func.max(PortfolioData.trade_date)).filter(PortfolioData.pl_id == _id)
                ), PortfolioData.pl_id == _id)
            ).all()
        elif status == 'recent':
            # count = request.args.get('count', 5, type=int)
            args = paginate_parser.parse_args()
            count = args['count']
            data_list = [date_2_str(d[0]) for d in db.session.query(
                PortfolioData.trade_date
            ).filter(PortfolioData.pl_id == _id).group_by(PortfolioData.trade_date).limit(count).all()]
            result_list = db.session.query(
                PortfolioData.trade_date, PortfolioData.asset_type, func.sum(PortfolioData.weight).label('weight')
            ).group_by(PortfolioData.trade_date, PortfolioData.asset_type).filter(
                and_(PortfolioData.trade_date.in_(data_list), PortfolioData.pl_id == _id)
            ).all()
        elif status == 'all':
            result_list = db.session.query(
                PortfolioData.trade_date, PortfolioData.asset_type, func.sum(PortfolioData.weight).label('weight')
            ).group_by(PortfolioData.trade_date, PortfolioData.asset_type).all()
        else:
            raise KeyError('status 参数错误 status = %s' % status)

        # 合并数据结果
        ret_dic_list = []
        ret_dic_dic = {}
        for data in result_list:
            trade_date = data.trade_date
            if trade_date in ret_dic_dic:
                pl_list, asset_name_list = ret_dic_dic[trade_date]
            else:
                pl_list = []
                asset_name_list = []
                ret_dic_dic[trade_date] = (pl_list, asset_name_list)
                ret_dic_list.append({
                    'trade_date': date_2_str(trade_date),
                    'data': pl_list,
                    'name_list': asset_name_list
                })

            # 扩展
            pl_list.append({
                'name': data.asset_type,
                'value': None if data.weight is None else float(data.weight),
            })
            asset_name_list.append(data.asset_type)

        ret_dic = {
            'count': len(ret_dic_list),
            'data': ret_dic_list,
        }
        # logger.debug("ret_dic:%s", ret_dic)
        return ret_dic


@api.route('/pl/<string:status>')
@api.param('status', 'my all star 其中之一')
class PortfolioListByStatusResource(Resource):

    @api.doc('获取投资组合列表数据（分页）')
    @api.expect(count_parser)
    @api.marshal_with(data_result_model)
    @login_required
    @api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
    def get(self, status):
        """
        获取投资组合列表数据
        """
        args = paginate_parser.parse_args()
        page_no = args['page_no']
        count = args['count']
        user_id = session.get('user_id')

        # 获取各个组合 最新交易日
        date_latest_query = db.session.query(
            PortfolioValueDaily.pl_id,
            func.max(PortfolioValueDaily.trade_date).label('trade_date_max')
        ).group_by(
            PortfolioValueDaily.pl_id).subquery('date_latest')
        # 获取各个投资组合 最新净值
        nav_latest_query = db.session.query(
            PortfolioValueDaily.pl_id,
            PortfolioValueDaily.trade_date,
            PortfolioValueDaily.rr,
            PortfolioValueDaily.nav
        ).filter(
            PortfolioValueDaily.pl_id == date_latest_query.c.pl_id,
            PortfolioValueDaily.trade_date == date_latest_query.c.trade_date_max
        ).subquery('nav_latest')

        # 分页查询投资组合信息及最新净值
        if status == 'my':
            filter_c = PortfolioInfo.create_user_id == user_id
        elif status == 'all':
            filter_c = or_(PortfolioInfo.access_type == 'public',
                           PortfolioInfo.create_user_id == user_id)
        elif status == 'star':
            # TODO: 星标投资组合
            filter_c = not_(func.isnull(FavoriteCompare.update_time))
        else:
            filter_c = None

        if filter_c is None:
            raise KeyError('status 参数错误 status = %s' % status)
        else:
            pagination = PortfolioInfo.query.outerjoin(
                nav_latest_query, PortfolioInfo.pl_id == nav_latest_query.c.pl_id
            ).add_columns(
                nav_latest_query.c.trade_date,
                nav_latest_query.c.rr,
                nav_latest_query.c.nav,
            ).outerjoin(User).add_columns(User.username).outerjoin(
                FavoritePortfolio,
                and_(PortfolioInfo.pl_id == FavoritePortfolio.pl_id, FavoritePortfolio.user_id == user_id)
            ).add_columns(
                func.if_(func.isnull(FavoritePortfolio.update_time), 0, 1).label('favorite')
            ).filter(
                filter_c
            ).paginate(page_no, count)

            logger.debug('%d / %d 页  %d / %d 条数据',
                         pagination.page, pagination.pages, len(pagination.items), pagination.total)
            ret_dic_list = [{
                'pl_id': data.PortfolioInfo.pl_id,
                'name': data.PortfolioInfo.name,
                'date_from': date_2_str(data.PortfolioInfo.date_from),
                'date_to': date_2_str(data.PortfolioInfo.date_to),
                'status': data.PortfolioInfo.status,
                'desc': data.PortfolioInfo.desc,
                'create_user_id': data.PortfolioInfo.create_user_id,
                'username': data.username,
                'favorite': data.favorite,
                'trade_date': date_2_str(data.trade_date),
                'rr': None if data.rr is None else float(data.rr),
                'nav': None if data.nav is None else float(data.nav),
                'access_type': data.PortfolioInfo.access_type,
            } for data in pagination.items]
            ret_dic = {
                'page': pagination.page,
                'pages': pagination.pages,
                'count': len(pagination.items),
                'total': pagination.total,
                'has_prev': pagination.has_prev,
                'has_next': pagination.has_next,
                'data': ret_dic_list,
            }
            return ret_dic


@api.route('/pl')
class PortfolioInfoResource(Resource):

    @api.doc('创建投资组合')
    @api.expect(login_parser, pl_create_model)
    @api.marshal_with(create_rsp_model)
    @login_required
    @api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
    def post(self):
        """
        创建投资组合
        """
        # logger.debug("request.json %s", request.json)
        # json_dic = request.json
        # if json_dic is None:
        #     return {"status": "error", "message": "no json"}
        # if "data" not in json_dic:
        #     return {"status": "error", "message": "'data' key in json"}
        # data_dic = json_dic["data"]
        data_dic = request.get_json() or request.form
        logger.debug("data_dic: %s", data_dic)

        # 添加投资组合信息
        data_obj = PortfolioInfo()
        populate_obj(data_obj, data_dic, attr_list=["name", "access_type", "desc"], error_if_no_key=True)
        # TODO: 需要进行：1）参数合法性检查 2）pl_id user_id create_dt 等参数不得传入，类似无效参数过滤
        user_id = session.get('user_id')
        data_obj.create_user_id = user_id
        db.session.add(data_obj)
        db.session.commit()
        logger.info('%s：id=%d 成功插入数据库 %s', data_obj.__class__.__name__, data_obj.pl_id, data_obj.__tablename__)

        # 添加投资组合
        if 'pl_data' in data_dic:
            pl_data_dic = data_dic['pl_data']
            add_pl_data(data_obj.pl_id, pl_data_dic)

        return {"status": "ok", 'id': data_obj.pl_id}


@api.route('/pl/data/<int:_id>')
class PortfolioDataUpdateResource(Resource):

    @api.doc('修改投资组合')
    @api.expect(login_parser, pl_data_model)
    @api.marshal_with(create_rsp_model)
    @login_required
    @api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_model)
    def put(self, _id):
        """
        修改投资组合
        """
        # data_dic = request.get_json() or request.form
        # logger.debug("data_dic(get_json): %s", data_dic)
        # data_dic = request.args
        # logger.debug("data_dic(args): %s", data_dic)
        data_dic = request.data
        logger.debug("data_dic(data): %s", data_dic)
        # data_dic = request.values
        # logger.debug("data_dic(values): %s", data_dic)

        # TODO: 增加权限检查，只能修改自己创建的投资组合
        # TODO: 交易日期必须大于等于当日，如果下午3点以后不得等于当日
        if data_dic is None:
            raise BadRequest('缺少json请求数据')
            # return jsonify({"status": "error", "message": "no json"})
        if "data" not in data_dic:
            # return jsonify({"status": "error", "message": "'data' key in json"})
            logger.error('请求数据：%s', data_dic)
            raise BadRequest('json 请求数据缺少 data 数据')
        # data = json_dic["data"]
        add_pl_data(_id, data_dic)

        return {"status": "ok", 'id': _id}


def add_pl_data(_id, pl_data_dic: dict):
    """
    更新或插入投资组合
    :param _id:
    :param pl_data_dic:
    :return:
    """
    date_str = pl_data_dic['trade_date']
    price_type = pl_data_dic['price_type']
    pl_data_dic_bulk = pl_data_dic['data']

    PortfolioData.query.filter(
        PortfolioData.pl_id == _id,
        PortfolioData.trade_date == date_str
    ).delete()
    # 获取上一个调仓日时的持仓纪录
    pl_data_obj_list_last_date = PortfolioData.query.filter(
        PortfolioData.pl_id == _id,
        PortfolioData.trade_date == (
            db.session.query(func.max(PortfolioData.trade_date)).filter(PortfolioData.pl_id == _id)
        )
    ).all()
    pl_data_obj_dic_last_date = {
        (data.asset_type, data.asset_code): data
        for data in pl_data_obj_list_last_date
    }

    # 建立持仓数据
    pl_data_obj_list = []
    for pl_d_dic in pl_data_dic_bulk:
        pl_d_obj = PortfolioData()
        pl_d_obj.pl_id = _id
        populate_obj(pl_d_obj, pl_d_dic, ["asset_code", "asset_type", "weight", "direction"])
        key = (pl_d_dic["asset_type"], pl_d_dic["asset_code"])
        pl_d_obj.weight_before = pl_data_obj_dic_last_date[key].weight if key in pl_data_obj_dic_last_date else 0
        pl_d_obj.trade_date = date_str
        pl_d_obj.price_type = price_type
        pl_data_obj_list.append(pl_d_obj)

    # 批量插入
    db.session.bulk_save_objects(pl_data_obj_list)
    db.session.commit()
    logger.debug("pl_id=%d: %d 投资组合数据插入到 pl_data 表", _id, len(pl_data_obj_list))


@api.route('/pl/stats/<int:_id>')
@api.param('_id', '投资组合ID')
class PortfolioStatisticResource(Resource):

    @login_required
    @api.expect(login_parser)
    @api.marshal_with(stats_model)
    def get(self, _id):
        """
        获取投资组合的绩效及统计信息
        """
        ret_data = {"general": {}, "performance": {}, "others": {}}
        pl_obj = db.session.query(PortfolioInfo, User.username).filter(PortfolioInfo.pl_id == _id,
                                                                       User.id == PortfolioInfo.create_user_id).first()
        ret_data['general']['pl_id'] = pl_obj.PortfolioInfo.pl_id
        ret_data['general']['name'] = pl_obj.PortfolioInfo.name
        ret_data['general']['date_from'] = date_2_str(pl_obj.PortfolioInfo.date_from)
        ret_data['general']['date_to'] = date_2_str(pl_obj.PortfolioInfo.date_to)
        ret_data['general']['create_user_id'] = pl_obj.PortfolioInfo.create_user_id
        ret_data['general']['create_user_name'] = pl_obj.username
        ret_data['general']['created_at'] = datetime_2_str(pl_obj.PortfolioInfo.create_dt)
        ret_data['general']['access_type'] = pl_obj.PortfolioInfo.access_type
        ret_data['general']['status'] = pl_obj.PortfolioInfo.status
        ret_data['general']['is_del'] = pl_obj.PortfolioInfo.is_del
        ret_data['general']['desc'] = pl_obj.PortfolioInfo.desc

        # date_latest = db.session.query(func.max(PortfolioValueDaily.trade_date)).filter(
        #     PortfolioValueDaily.pl_id == _id).scalar()
        pl_df = pd.read_sql(
            "select trade_date, nav from " + PortfolioValueDaily.__tablename__ + "  where pl_id = %s order by trade_date",
            db.engine, params=[_id], index_col='trade_date')
        if pl_df.shape[0] > 0:
            stat_dic_dic = calc_performance(pl_df, freq=None)
            if 'nav' in stat_dic_dic:
                performance_dic = stat_dic_dic['nav']
                for key in list(performance_dic.keys()):
                    value = performance_dic[key]
                    if value is None:
                        performance_dic[key] = '-'
                    elif type(value) is date:
                        performance_dic[key] = date_2_str(value)
                    elif type(value) is not str and not np.isfinite(value):
                        performance_dic[key] = '-'
                ret_data['performance'] = stat_dic_dic['nav']
            else:
                logger.warning('stat_dic_dic:\n%s', stat_dic_dic)

        # 星标数量
        star_count = db.session.query(func.count()).filter(FavoritePortfolio.pl_id == _id).scalar()
        ret_data['others']['star_count'] = int(star_count)

        # 最新净值
        latest_nav_sub_query = db.session.query(PortfolioValueDaily.nav).filter(
            PortfolioValueDaily.pl_id == _id,
            PortfolioValueDaily.trade_date == (
                db.session.query(func.max(PortfolioValueDaily.trade_date)).filter(PortfolioValueDaily.pl_id == _id)
            )
        ).subquery()

        # 收益率排名
        max_trade_date_sub_query = db.session.query(
            PortfolioValueDaily.pl_id, func.max(PortfolioValueDaily.trade_date).label('trade_date_max')
        ).group_by(PortfolioValueDaily.pl_id).subquery()
        latest_nav_list_sub_query = db.session.query(PortfolioValueDaily.pl_id, PortfolioValueDaily.nav).join(
            max_trade_date_sub_query,
            and_(
                PortfolioValueDaily.pl_id == max_trade_date_sub_query.c.pl_id,
                PortfolioValueDaily.trade_date == max_trade_date_sub_query.c.trade_date_max
            )
        ).order_by(PortfolioValueDaily.nav.asc()).subquery()

        # 净值排名
        nav_rank_count = db.session.query(func.count()).filter(
            latest_nav_list_sub_query.c.nav >= latest_nav_sub_query.c.nav).scalar()
        # 总产品数量
        pl_count_has_nav = db.session.query(
            func.count(
                db.session.query(PortfolioValueDaily.pl_id).group_by(PortfolioValueDaily.pl_id).subquery().c.pl_id)
        ).scalar()
        ret_data['others']['rank'] = 0 if pl_count_has_nav == 0 else float(nav_rank_count / pl_count_has_nav)
        return ret_data


@api.route('/pl/favorite/<int:_id>/<int:do>')
class CompareFavorite(Resource):

    @api.doc('添加、修改星标')
    @api.expect(login_parser)
    @api.marshal_with(create_rsp_model)
    @login_required
    @api.response(HTTPStatus.BAD_REQUEST, "参数错误", model=error_action_model)
    def post(self, _id: int, do: int):
        """
        添加、修改星标
        """
        user_id = session.get('user_id')
        if do == 0:
            FavoritePortfolio.query.filter(FavoritePortfolio.pl_id == _id,
                                           FavoritePortfolio.user_id == user_id).delete()
            db.session.commit()
            return {'status': 'ok'}
        else:
            data_obj = FavoritePortfolio()
            data_obj.pl_id = _id
            data_obj.user_id = user_id
            db.session.add(data_obj)
            db.session.commit()
            logger.info('%s：id=%d 成功插入数据库 %s', data_obj.__class__.__name__, data_obj.id, data_obj.__tablename__)

            return {"status": "ok", 'id': data_obj.id}


@api.route('/pl/info/<int:_id>')
@api.param('_id', '投资组合ID')
class PortfolioInfoActionResource(Resource):

    # @api.doc('删除投资组合')
    @api.expect(login_parser)
    @api.marshal_with(delete_rsp_model)
    @login_required
    @api.response(HTTPStatus.UNAUTHORIZED, "无权进行此操作", model=error_action_model)
    def delete(self, _id):
        """
        删除指定投资组合
        """
        logger.debug('pl_id=%d', _id)
        PortfolioInfo.query.filter(PortfolioInfo.pl_id == _id).update({'is_del': 1})
        db.session.commit()
        return {'status': 'ok', 'id': _id}
