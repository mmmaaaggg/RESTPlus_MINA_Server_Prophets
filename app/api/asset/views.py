#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/6/5 15:27
@File    : views.py
@contact : mmmaaaggg@163.com
@desc    : 
"""
from flask_restplus._http import HTTPStatus
from app.api.asset import api, logger
from app import db
from app.config import config
import pandas as pd
from flask_restplus import Resource, fields, reqparse

error_model = api.model('error', {
    'data': fields.List(fields.Raw),
    'status': fields.String(description='状态'),
    'message': fields.String(description='错误信息'),
})
asset_model = api.model('asset_model', {
    'text': fields.String(required=True, description='资产类型名称'),
    'children': fields.List(fields.Nested(api.model(
        'asset_type_name',
        {
            'asset_type': fields.String(required=True, description='资产类型'),
            'asset_name': fields.String(required=True, description='资产名称'),
            'text': fields.String(required=True, description='显示名称'),
        }
    )))
})
asset_results_model = api.model(
    'asset_results', {
        # 'results': fields.String(required=True, description='资产类型'),
        'results': fields.List(fields.Nested(asset_model)),
    }
)

# fields.Nested(api.model(
#         '资产历史K线',
#         {
#             'trade_date': fields.Date(description='交易日期(D)'),
#             'nav_acc': fields.Float(description='累计净值'),
#             'open': fields.Float(description='开盘价(O)'),
#             'high': fields.Float(description='最高价(H)'),
#             'low': fields.Float(description='最低价(L)'),
#             'close': fields.Float(description='收盘价(C)'),
#             'volume': fields.Float(description='成交量(V)'),
#             'amt': fields.Float(description='成交额(A)'),
#         }
#     ))
# asset_candle_model = api.model('K线', fields.List(fields.Raw))
data_result_model = api.model('asset_candle_result_model', {
    'data': fields.List(fields.List(fields.Raw)),
    'count': fields.Integer(description='记录数')
})


@api.route('/asset/<string:search_term>')
@api.param('search_term', '匹配字符串')
class Asset(Resource):

    @api.doc('查询符合条件资产列表')
    @api.marshal_with(asset_results_model)
    def get(self, search_term):
        """
        查询按资产类别分类的资产列表，
        返回匹配相关字符的全部分组结果（供搜索使用只返回前50条记录）
        """
        ret_dic = {
            "results": []
        }
        logger.debug('搜索条件：%s', search_term)
        param_str = "%%" + search_term + '%%'
        # 查找投资组合
        sql_str = """SELECT 'portfolio' AS asset_type, pl_id asset_code,
         name asset_name, concat(pl_id, ' : ', pl_info.name) text 
         FROM pl_info WHERE pl_info.name LIKE %s LIMIT 50"""
        data_df = pd.read_sql(sql_str, db.engine, params=[param_str])
        logger.debug('投资组合数据 %d 条数据', data_df.shape[0])
        if data_df.shape[0] > 0:
            data_dic = data_df.to_dict('record')
            ret_dic['results'].append(
                {
                    "text": "投资组合",
                    "children": data_dic
                }
            )
        # 查找指数
        sql_str = """SELECT 'index' AS asset_type, wind_code asset_code,
        sec_name asset_name, concat(wind_code, ' : ',sec_name) text
        FROM wind_index_info WHERE sec_name LIKE %s OR wind_code LIKE %s LIMIT 50"""
        data_df = pd.read_sql(sql_str,
                              db.get_engine(db.get_app(), bind=config.BIND_DB_NAME_MD),
                              params=[param_str, param_str])
        logger.debug('指数数据 %d 条数据', data_df.shape[0])
        if data_df.shape[0] > 0:
            data_dic = data_df.to_dict('record')
            ret_dic['results'].append(
                {
                    "text": "指数",
                    "children": data_dic
                }
            )
        # 查找股票
        sql_str = """SELECT 'stock' AS asset_type, wind_code asset_code, 
        sec_name asset_name, concat(wind_code, ' : ',sec_name) text 
        FROM wind_stock_info WHERE sec_name LIKE %s OR prename LIKE %s OR wind_code LIKE %s LIMIT 50"""
        data_df = pd.read_sql(sql_str,
                              db.get_engine(db.get_app(), bind=config.BIND_DB_NAME_MD),
                              params=[param_str, param_str, param_str])
        logger.debug('股票数据 %d 条数据', data_df.shape[0])
        if data_df.shape[0] > 0:
            data_dic = data_df.to_dict('record')
            ret_dic['results'].append(
                {
                    "text": "股票",
                    "children": data_dic
                }
            )
        # 查找期货
        sql_str = """SELECT 'future' AS asset_type, wind_code asset_code, 
        sec_name asset_name, concat(wind_code, ' : ',sec_name) text
        FROM wind_future_info WHERE sec_name LIKE %s OR wind_code LIKE %s LIMIT 50"""
        data_df = pd.read_sql(sql_str,
                              db.get_engine(db.get_app(), bind=config.BIND_DB_NAME_MD),
                              params=[param_str, param_str])
        logger.debug('期货数据 %d 条数据', data_df.shape[0])
        if data_df.shape[0] > 0:
            data_dic = data_df.to_dict('record')
            ret_dic['results'].append(
                {
                    "text": "期货",
                    "children": data_dic
                }
            )

        # logger.debug(ret_dic)
        # return jsonify(ret_dic)
        return ret_dic


def get_asset_name(asset_type, asset_code):
    """
    根据 asset_type, asset_code 查询相应的股票、期货、基金、指数等对应名称
    :param asset_type:
    :param asset_code:
    :return:
    TODO: 增加 cache 机制
    """
    engine_md = db.get_engine(app=db.get_app(), bind=config.BIND_DB_NAME_MD)
    if asset_type == 'portfolio':
        row = db.engine.execute("SELECT name FROM pl_info WHERE pl_id = %s", asset_code).first()
    elif asset_type == 'future':
        row = engine_md.execute("SELECT sec_name FROM wind_future_info WHERE wind_code = %s", asset_code).first()
    elif asset_type == 'stock':
        # engine_md.execute("select sec_name from wind_stock_info where wind_code = %s", '000006.SZ').first()
        row = engine_md.execute("SELECT sec_name FROM wind_stock_info WHERE wind_code = %s", asset_code).first()
    elif asset_type == 'index':
        row = engine_md.execute("SELECT sec_name FROM wind_index_info WHERE wind_code = %s", asset_code).first()
    elif asset_type == 'fund':
        row = engine_md.execute("SELECT sec_name FROM wind_fund_info WHERE wind_code = %s", asset_code).first()
    else:
        row = None

    if row is None:
        return None
    else:
        return row[0]


@api.route('/candle/<string:asset_type>/<string:asset_code>/<string:item_order>')
@api.param('asset_type', '资产类型')
@api.param('asset_code', '资产代码')
@api.param('item_order', '返回列顺序 默认DOHLCVA，分别对应 date, open, high, low, close, vol, amt')
@api.response(404, "item_order 参数错误", model=error_model)
class AssetCandle(Resource):

    @api.marshal_with(data_result_model)
    def get(self, asset_type, asset_code, item_order='DOHLCVA'):
        """
        查询相关资产历史数据
        """
        engine_md = db.get_engine(app=db.get_app(), bind=config.BIND_DB_NAME_MD)
        item_name_map = {
            'D': 'trade_date',
            'O': 'open',
            'H': 'high',
            'L': 'low',
            'C': 'close',
            'V': 'volume',
            'A': 'amt',
        }
        if asset_type == 'portfolio':
            data_df = pd.read_sql("""select * from (select DATE_FORMAT(trade_date, "%%Y-%%m-%%d") trade_date, nav
                from pl_value_daily 
                where pl_id = %s
                order by trade_date desc limit 100) t ORDER BY trade_date""", db.engine, params=[asset_code])
            item_names = ['trade_date', 'nav']
        elif asset_type == 'future':
            if not all([c in item_name_map for c in item_order]):
                return {'data': [], 'status': 'error', 'message': '%s 不是有效参数' % item_order}, 404
            data_df = pd.read_sql("""select * from (select DATE_FORMAT(trade_date, "%%Y-%%m-%%d") trade_date, 
                open, high, low, close, volume, amt 
                from wind_future_daily 
                where wind_code = %s
                order by trade_date desc limit 100) t ORDER BY trade_date""", engine_md, params=[asset_code])
            item_names = [item_name_map[k] for k in item_order]
        elif asset_type == 'stock':
            if not all([c in item_name_map for c in item_order]):
                return {'data': [], 'status': 'error', 'message': '%s 不是有效参数' % item_order}, 404
            # engine_md.execute("select sec_name from wind_stock_info where wind_code = %s", '000006.SZ').first()
            data_df = pd.read_sql("""select * from (select DATE_FORMAT(trade_date, "%%Y-%%m-%%d") trade_date, 
                open, high, low, close, volume, amt 
                from wind_stock_daily 
                where wind_code = %s
                order by trade_date desc limit 100) t ORDER BY trade_date""", engine_md, params=[asset_code])
            item_names = [item_name_map[k] for k in item_order]
        elif asset_type == 'index':
            if not all([c in item_name_map for c in item_order]):
                return {'data': [], 'status': 'error', 'message': '%s 不是有效参数' % item_order}, 404
            data_df = pd.read_sql("""select * from (select DATE_FORMAT(trade_date, "%%Y-%%m-%%d") trade_date, 
                open, high, low, close, volume, amt 
                from wind_index_daily 
                where wind_code = %s
                order by trade_date desc limit 100) t ORDER BY trade_date""", engine_md, params=[asset_code])
            item_names = [item_name_map[k] for k in item_order]
        elif asset_type == 'fund':
            data_df = pd.read_sql("""select * from (select DATE_FORMAT(nav_date, "%%Y-%%m-%%d") trade_date, nav_acc
                from wind_fund_nav 
                where wind_code = %s
                group by nav_date
                order by nav_date desc limit 100) t ORDER BY trade_date""", engine_md, params=[asset_code])
        else:
            data_df = None

        # ret_data = [list(x) for x in data_df.as_matrix()]
        ret_dic = {'data': [list(x) for x in data_df[item_names].as_matrix()] if data_df is not None else [],
                   'count': data_df.shape[0] if data_df is not None else 0
                   }
        return ret_dic


if __name__ == "__main__":
    name = get_asset_name('stock', '000006.SZ')
