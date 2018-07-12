#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/3/30 17:47
@File    : forecast.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from datetime import timedelta
from flask_restplus import Resource, fields, reqparse
from app.api.forecast import api
from app import db
from app.config import config
from flask import render_template, request, session, url_for, redirect, jsonify
from flask_login import login_required
import json
import logging
from app.api.auth.models import User
from app.api.forecast.models import PortfolioInfo, PortfolioData, PortfolioValueDaily, \
    PortfolioCompareResult, PortfolioCompareInfo, FavoriteCompare, FavoritePortfolio
from app.api.asset.views import get_asset_name
# from flask.views import MethodView
from app.utils.fh_utils import date_2_str, populate_obj, calc_performance, datetime_2_str
import pandas as pd
import numpy as np
from sqlalchemy import func, or_, and_, column, not_
from datetime import date
logger = logging.getLogger()

# 相关 parser 文件
paginate_model = api.model('paginate', {
    'page': fields.Integer(required=True, description='当前页码', default=1),
    'pages': fields.Integer(required=True, description='总共页数', default=1),
    'count': fields.Integer(required=True, description='当前记录数', default=1),
    'total': fields.Integer(required=True, description='总共记录数', default=1),
    'has_prev': fields.Integer(required=True, description='有前一页', default=1),
    'has_next': fields.Integer(required=True, description='有后一页', default=1),
    'data': fields.Integer(required=True, description='数据记录', default=1),
})


@api.route('/cmp/<string:status>')
@api.param('status', 'my all star verified unverified 其中之一')
@login_required
class CmpWithStatus(Resource):

    @api.doc('获取比较列表数据（分页）')
    # @api.expect(paginate_parser)
    @api.marshal_with(paginate_model)
    def get(self, status):
        """
        获取比较列表数据（分页）
        :param status:
        :return:
        """
        args = dict()  # paginate_parser.parse_args()
        page_no = args['page_no']
        count = args['count']
        user_id = session.get('user_id')
        logger.debug('get_cmp_list user_id:%d', user_id)
        if status == 'my':
            filter_c = PortfolioCompareInfo.create_user_id == user_id
            having_c = None
            # sql_str = """-- select * from
            #     select cmp_info.cmp_id, name, status, params, cmp_info.desc,
            #     fit_count, tot_count, fit_count/tot_count fit_rate,
            #     DATE_FORMAT(date_from, "%%Y-%%m-%%d") date_from,
            #     DATE_FORMAT(date_to, "%%Y-%%m-%%d") date_to,
            #     DATE_FORMAT(trade_date_min, "%%Y-%%m-%%d") trade_date_min,
            #     DATE_FORMAT(trade_date_max, "%%Y-%%m-%%d") trade_date_max,
            #     datediff(trade_date_max, date_from) / datediff(date_to, date_from) complete_rate, create_user_id, username
            #     from
            #     (
            #       select * from pl_compare_info where create_user_id = %s and is_del=0
            #     ) cmp_info
            #     left join
            #     (
            #       select cmp_id, min(trade_date) trade_date_min, max(trade_date) trade_date_max,
            #       sum(result) fit_count, count(*) tot_count
            #       from pl_compare_result group by cmp_id
            #     ) cmp_rst
            #     on cmp_info.cmp_id = cmp_rst.cmp_id
            #     left join
            #     user
            #     on cmp_info.create_user_id = user.id"""
            # data_list_df = pd.read_sql(sql_str, db.engine, params=[user_id])
        elif status == 'all':
            filter_c = or_(PortfolioCompareInfo.create_user_id == user_id, PortfolioCompareInfo.access_type == 'public')
            having_c = None
            # sql_str = """-- select * from
            #     select cmp_info.cmp_id, name, status, params, cmp_info.desc,
            #     fit_count, tot_count, fit_count/tot_count fit_rate,
            #     DATE_FORMAT(date_from, "%%Y-%%m-%%d") date_from,
            #     DATE_FORMAT(date_to, "%%Y-%%m-%%d") date_to,
            #     DATE_FORMAT(trade_date_min, "%%Y-%%m-%%d") trade_date_min,
            #     DATE_FORMAT(trade_date_max, "%%Y-%%m-%%d") trade_date_max,
            #     datediff(trade_date_max, date_from) / datediff(date_to, date_from) complete_rate, create_user_id, username
            #     from
            #     (
            #       select * from pl_compare_info where (create_user_id = %s or access_type='public') and is_del=0
            #     ) cmp_info
            #     left join
            #     (
            #       select cmp_id, min(trade_date) trade_date_min, max(trade_date) trade_date_max,
            #       sum(result) fit_count, count(*) tot_count
            #       from pl_compare_result group by cmp_id
            #     ) cmp_rst
            #     on cmp_info.cmp_id = cmp_rst.cmp_id
            #     left join
            #     user
            #     on cmp_info.create_user_id = user.id"""
            # data_list_df = pd.read_sql(sql_str, db.engine, params=[user_id])
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
            filter_c = None
            having_c = None
        # 整理数据
        # logger.debug("data_list_df len:%d", data_list_df.shape[0])
        # data_list_df = data_list_df.where(data_list_df.notna(), None)
        # data_list = data_list_df.to_dict('record')
        # data_table_dic = {'data': data_list}
        # logger.debug(data_table_dic)

        if filter_c is None:
            return jsonify({})
        else:
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
            return jsonify(ret_dic)


@api.route('/cmp/get_list/<string:status>')
@login_required
def get_cmp_list(status=None):
    """
    获取比较列表数据（分页）
    :param status:
    :return:
    """
    page_no = request.args.get('page_no', 1, type=int)
    count = request.args.get('count', config.APP_PAGINATE_ITEMS_COUNT, type=int)
    user_id = session.get('user_id')
    logger.debug('get_cmp_list user_id:%d', user_id)
    if status == 'my':
        filter_c = PortfolioCompareInfo.create_user_id == user_id
        having_c = None
        # sql_str = """-- select * from
        #     select cmp_info.cmp_id, name, status, params, cmp_info.desc,
        #     fit_count, tot_count, fit_count/tot_count fit_rate,
        #     DATE_FORMAT(date_from, "%%Y-%%m-%%d") date_from,
        #     DATE_FORMAT(date_to, "%%Y-%%m-%%d") date_to,
        #     DATE_FORMAT(trade_date_min, "%%Y-%%m-%%d") trade_date_min,
        #     DATE_FORMAT(trade_date_max, "%%Y-%%m-%%d") trade_date_max,
        #     datediff(trade_date_max, date_from) / datediff(date_to, date_from) complete_rate, create_user_id, username
        #     from
        #     (
        #       select * from pl_compare_info where create_user_id = %s and is_del=0
        #     ) cmp_info
        #     left join
        #     (
        #       select cmp_id, min(trade_date) trade_date_min, max(trade_date) trade_date_max,
        #       sum(result) fit_count, count(*) tot_count
        #       from pl_compare_result group by cmp_id
        #     ) cmp_rst
        #     on cmp_info.cmp_id = cmp_rst.cmp_id
        #     left join
        #     user
        #     on cmp_info.create_user_id = user.id"""
        # data_list_df = pd.read_sql(sql_str, db.engine, params=[user_id])
    elif status == 'all':
        filter_c = or_(PortfolioCompareInfo.create_user_id == user_id, PortfolioCompareInfo.access_type == 'public')
        having_c = None
        # sql_str = """-- select * from
        #     select cmp_info.cmp_id, name, status, params, cmp_info.desc,
        #     fit_count, tot_count, fit_count/tot_count fit_rate,
        #     DATE_FORMAT(date_from, "%%Y-%%m-%%d") date_from,
        #     DATE_FORMAT(date_to, "%%Y-%%m-%%d") date_to,
        #     DATE_FORMAT(trade_date_min, "%%Y-%%m-%%d") trade_date_min,
        #     DATE_FORMAT(trade_date_max, "%%Y-%%m-%%d") trade_date_max,
        #     datediff(trade_date_max, date_from) / datediff(date_to, date_from) complete_rate, create_user_id, username
        #     from
        #     (
        #       select * from pl_compare_info where (create_user_id = %s or access_type='public') and is_del=0
        #     ) cmp_info
        #     left join
        #     (
        #       select cmp_id, min(trade_date) trade_date_min, max(trade_date) trade_date_max,
        #       sum(result) fit_count, count(*) tot_count
        #       from pl_compare_result group by cmp_id
        #     ) cmp_rst
        #     on cmp_info.cmp_id = cmp_rst.cmp_id
        #     left join
        #     user
        #     on cmp_info.create_user_id = user.id"""
        # data_list_df = pd.read_sql(sql_str, db.engine, params=[user_id])
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
        filter_c = None
        having_c = None
    # 整理数据
    # logger.debug("data_list_df len:%d", data_list_df.shape[0])
    # data_list_df = data_list_df.where(data_list_df.notna(), None)
    # data_list = data_list_df.to_dict('record')
    # data_table_dic = {'data': data_list}
    # logger.debug(data_table_dic)

    if filter_c is None:
        return jsonify({})
    else:
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
        return jsonify(ret_dic)


@api.route('/cmp/get_rst/<string:cmp_id>')
def get_cmp_rst(cmp_id):
    """
    返回预测结果数据（供 charts 使用，无需分页返回）
    :param cmp_id:
    :return:
    """
    # page_no = request.args.get('page_no', 1, type=int)
    # count = request.args.get('count', config.APP_PAGINATE_ITEMS_COUNT, type=int)
    # pagination = PortfolioCompareResult.query.filter(PortfolioCompareResult.cmp_id == cmp_id)\
    #     .paginate(page_no, count)
    # logger.debug('%d / %d 页  %d / %d 条数据',
    #              pagination.page, pagination.pages, len(pagination.items), pagination.total)
    # ret_dic_list = [{
    #     'trade_date': date_2_str(data.trade_date),
    #     'asset_1': data.asset_1,
    #     'asset_2': data.asset_2,
    #     'asset_3': data.asset_3,
    #     'result': data.result,
    #     'shift_value': data.shift_value,
    #     'shift_rate': data.shift_rate,
    # } for data in pagination.items]
    # ret_dic = {
    #     'page': pagination.page,
    #     'pages': pagination.pages,
    #     'count': len(pagination.items),
    #     'total': pagination.total,
    #     'has_prev': pagination.has_prev,
    #     'has_next': pagination.has_next,
    #     'data': ret_dic_list,
    # }
    sql_str = """SELECT DATE_FORMAT(trade_date, "%%Y-%%m-%%d") trade_date,
    asset_1, asset_2, asset_3, result, shift_value, shift_rate FROM pl_compare_result
    WHERE cmp_id = %s"""
    data_df = pd.read_sql(sql_str, db.engine, params=[cmp_id])
    ret_df = data_df.where(data_df.notna(), None)
    ret_dic_list = ret_df.to_dict('list')
    logger.debug('%d 条数据', ret_df.shape[0])
    ret_dic = {
        'data': ret_dic_list,
    }
    return jsonify(ret_dic)


@api.route('/pl/get_data_list/<_id>/<string:status>')
def get_pl_data_list(_id, status):
    """
    获取制定投资组合的成分及权重数据（分页）
    status
    :param _id:
    :param status: latest 最近一次调仓数据, recent 最近几日调仓数据，日期逆序排列
    :return:
    """
    logger.info('pl_id=%s, status=%s', _id, status)

    # sql_str = """SELECT asset_code, asset_type, DATE_FORMAT(trade_date, "%%Y-%%m-%%d") trade_date, weight
    #     FROM pl_data where pl_id=%s
    #     and trade_date = (select max(trade_date) from pl_data where pl_id=%s)"""
    # data_list_df = pd.read_sql(sql_str, db.engine, params=[pl_id, pl_id])
    # # 整理数据
    # logger.debug("data_list_df len:%d", data_list_df.shape[0])
    # data_list_df = data_list_df.where(data_list_df.notna(), None)
    # data_list = data_list_df.to_dict('record')
    # data_table_dic = {'data': data_list}
    page_no = request.args.get('page_no', 1, type=int)
    count = request.args.get('count', config.APP_PAGINATE_ITEMS_COUNT, type=int)

    # PortfolioData.query.filter(PortfolioData.id == pl_id, PortfolioData.trade_date == 1)
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
    return jsonify(ret_dic)


@api.route('/pl/get_data_list_by_date/<int:_id>/<string:status>')
@login_required
def get_pl_data_list_by_date(_id, status):
    """
    获取制定投资组合的成分及权重数据（分页）
    status
    :param _id:
    :param status: latest 最近一次调仓数据, recent 最近几日调仓数据，日期逆序排列
    :return:
    """
    logger.info('pl_id=%s, status=%s', _id, status)
    page_no = request.args.get('page_no', 1, type=int)
    count = request.args.get('count', config.APP_PAGINATE_ITEMS_COUNT, type=int)

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
    return jsonify(ret_dic)


@api.route('/pl/asset_dist/<pl_id>/<string:status>')
def get_pl_asset_dist(pl_id, status):
    """
    投资组合资产分布比例
    :param pl_id:
    :param status: 'latest'  'recent' 'all'
    :return:
    """
    logger.info('pl_id=%s, status=%s', pl_id, status)
    if status == 'latest':
        result_list = db.session.query(
            PortfolioData.trade_date, PortfolioData.asset_type, func.sum(PortfolioData.weight).label('weight')
        ).group_by(PortfolioData.trade_date, PortfolioData.asset_type).filter(
            and_(PortfolioData.trade_date == (
                db.session.query(func.max(PortfolioData.trade_date)).filter(PortfolioData.pl_id == pl_id)
            ), PortfolioData.pl_id == pl_id)
        ).all()
    elif status == 'recent':
        count = request.args.get('count', 5, type=int)
        data_list = [date_2_str(d[0]) for d in db.session.query(
            PortfolioData.trade_date
        ).filter(PortfolioData.pl_id == pl_id).group_by(PortfolioData.trade_date).limit(count).all()]
        result_list = db.session.query(
            PortfolioData.trade_date, PortfolioData.asset_type, func.sum(PortfolioData.weight).label('weight')
        ).group_by(PortfolioData.trade_date, PortfolioData.asset_type).filter(
            and_(PortfolioData.trade_date.in_(data_list), PortfolioData.pl_id == pl_id)
        ).all()
    elif status == 'all':
        result_list = db.session.query(
            PortfolioData.trade_date, PortfolioData.asset_type, func.sum(PortfolioData.weight).label('weight')
        ).group_by(PortfolioData.trade_date, PortfolioData.asset_type).all()
    else:
        return jsonify({})

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
    return jsonify(ret_dic)


@api.route('/pl/get_list/<string:status>')
@login_required
def get_pl_list(status):
    """
    获取投资组合列表数据
    :param status:
    :return:
    """
    page_no = request.args.get('page_no', 1, type=int)
    count = request.args.get('count', config.APP_PAGINATE_ITEMS_COUNT, type=int)
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
        return jsonify({})
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
        return jsonify(ret_dic)


@api.route('/pl/create', methods=['POST'])
@login_required
def create_pl_info():
    """
    创建投资组合
    :return:
    """
    logger.debug("request.json %s", request.json)
    json_dic = request.json
    if json_dic is None:
        return jsonify({"status": "error", "message": "no json"})
    if "data" not in json_dic:
        return jsonify({"status": "error", "message": "'data' key in json"})
    data_dic = json_dic["data"]

    # 添加投资组合信息
    data_obj = PortfolioInfo()
    try:
        populate_obj(data_obj, data_dic, attr_list=["name", "access_type", "desc"], error_if_no_key=True)
    except KeyError as exp:
        logger.exception("")
        return jsonify({"status": "error", "message": exp.args[0]})
    # TODO: 需要进行：1）参数合法性检查 2）pl_id user_id create_dt 等参数不得传入，类似无效参数过滤
    user_id = session.get('user_id')
    data_obj.create_user_id = user_id
    try:
        db.session.add(data_obj)
        db.session.commit()
        logger.info('%s：id=%d 成功插入数据库 %s', data_obj.__class__.__name__, data_obj.pl_id, data_obj.__tablename__)
    except Exception as exp:
        return jsonify({"status": "error", "message": exp.args[0]})

    # 添加投资组合
    if 'pl_data' in data_dic:
        pl_data_dic = data_dic['pl_data']
        add_pl_data(data_obj.pl_id, pl_data_dic)

    return jsonify({"status": "ok", 'id': data_obj.pl_id})


@api.route('/pl_data/update/<int:_id>', methods=['POST'])
@login_required
def updata_pl_data(_id):
    """
    修改投资组合
    :param _id:
    :return:
    """
    # TODO: 增加权限检查，只能修改自己创建的投资组合
    # TODO: 交易日期必须大于等于当日，如果下午3点以后不得等于当日
    logger.debug("request.json %s", request.json)
    json_dic = request.json
    if json_dic is None:
        return jsonify({"status": "error", "message": "no json"})
    if "data" not in json_dic:
        return jsonify({"status": "error", "message": "'data' key in json"})
    data = json_dic["data"]
    add_pl_data(_id, data)

    return jsonify({"status": "ok", 'id': _id})


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


@api.route('/cmp/create', methods=['POST'])
@login_required
def create_cmp_info():
    logger.debug("request.json %s", request.json)
    json_dic = request.json
    if json_dic is None:
        return jsonify({"status": "error", "message": "no json"})
    if "data" not in json_dic:
        return jsonify({"status": "error", "message": "'data' key in json"})
    data_dic = json_dic["data"]

    # 添加投资组合信息
    data_obj = PortfolioCompareInfo()
    try:
        populate_obj(data_obj, data_dic,
                     attr_list=["name", "date_from", "date_to", "access_type", "desc"],
                     error_if_no_key=True)
        data_obj.params = json.dumps(data_dic['params'])
    except KeyError as exp:
        logger.exception('对 PortfolioCompareInfo 对象赋值失败')
        return jsonify({"status": "error", "message": exp.args[0]})
    # TODO: 需要进行：1）参数合法性检查 2）pl_id user_id create_dt 等参数不得传入，类似无效参数过滤
    user_id = session.get('user_id')
    data_obj.create_user_id = user_id
    try:
        db.session.add(data_obj)
        db.session.commit()
        logger.info('%s：id=%d 成功插入数据库 %s', data_obj.__class__.__name__, data_obj.cmp_id, data_obj.__tablename__)
    except Exception as exp:
        logger.exception('创建预测失败')
        return jsonify({"status": "error", "message": exp.args[0]})

    return jsonify({"status": "ok", 'id': data_obj.cmp_id})


@api.route('/cmp/summary')
@login_required
def get_cmp_summary():
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
    unverified, verified = raw

    favorite = db.session.query(func.count(PortfolioCompareInfo.cmp_id)).join(
        FavoriteCompare,
        and_(PortfolioCompareInfo.cmp_id == FavoriteCompare.cmp_id, FavoriteCompare.user_id == user_id)
    ).scalar()

    ret_data = [
        {
            "name": "待验证",
            "status": "unverified",
            "count": float(unverified)
        },
        {
            "name": "已验证",
            "status": "verified",
            "count": float(verified)
        },
        {
            "name": "关注预言",
            "status": "favorite",
            "count": float(favorite)
        }
    ]
    return jsonify(ret_data)


@api.route('/pl/stats/<int:_id>')
@login_required
def get_pl_stats(_id):
    """
    获取投资组合的绩效及统计信息
    :param _id:
    :return:
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
    stat_dic_dic = calc_performance(pl_df, freq=None)
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
    ret_data['others']['rank'] = float(nav_rank_count / pl_count_has_nav)
    return jsonify(ret_data)


@api.route('/cmp/favorite/<_id>/<int:do>', methods=['POST'])
@login_required
def favorite_cmp(_id: int, do: int):
    """

    :param _id:
    :param do: 0：取消，其它：关注
    :return:
    """
    user_id = session.get('user_id')
    if do == 0:
        FavoriteCompare.query.filter(FavoriteCompare.cmp_id == _id, FavoriteCompare.user_id == user_id).delete()
        db.session.commit()
        return jsonify({'status': 'ok'})
    else:
        data_obj = FavoriteCompare()
        data_obj.cmp_id = _id
        data_obj.user_id = user_id
        try:
            db.session.add(data_obj)
            db.session.commit()
            logger.info('%s：id=%d 成功插入数据库 %s', data_obj.__class__.__name__, data_obj.id, data_obj.__tablename__)
        except Exception as exp:
            return jsonify({"status": "error", "message": exp.args[0]})

        return jsonify({"status": "ok", 'id': data_obj.id})


@api.route('/pl/favorite/<_id>/<int:do>', methods=['POST'])
@login_required
def favorite_pl(_id: int, do: int):
    """

    :param _id:
    :param do: 0：取消，其它：关注
    :return:
    """
    user_id = session.get('user_id')
    if do == 0:
        FavoritePortfolio.query.filter(FavoritePortfolio.pl_id == _id, FavoritePortfolio.user_id == user_id).delete()
        db.session.commit()
        return jsonify({'status': 'ok'})
    else:
        data_obj = FavoritePortfolio()
        data_obj.pl_id = _id
        data_obj.user_id = user_id
        try:
            db.session.add(data_obj)
            db.session.commit()
            logger.info('%s：id=%d 成功插入数据库 %s', data_obj.__class__.__name__, data_obj.id, data_obj.__tablename__)
        except Exception as exp:
            return jsonify({"status": "error", "message": exp.args[0]})

        return jsonify({"status": "ok", 'id': data_obj.id})


@api.route('/pl/del/<int:pl_id>', methods=['POST'])
def del_pl_info(pl_id):
    logger.debug('pl_id=%d', pl_id)
    PortfolioInfo.query.filter(PortfolioInfo.pl_id == pl_id).update({'is_del': 1})
    db.session.commit()
    return jsonify({'status': 'ok'})


@api.route('/cmp/del/<int:cmp_id>', methods=['POST'])
def del_cmp_info(cmp_id):
    logger.debug('cmp_id=%d', cmp_id)
    PortfolioCompareInfo.query.filter(PortfolioCompareInfo.cmp_id == cmp_id).update({'is_del': 1})
    db.session.commit()
    return jsonify({'status': 'ok'})


if __name__ == "__main__":
    from app.auth.views import force_login_by_wx_user
