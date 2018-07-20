#! /usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author  : MG
@Time    : 2018/4/2 9:56
@File    : data_analysis.py
@contact : mmmaaaggg@163.com
@desc    : 通过后台现场对数据进行定期分析
"""
from collections import OrderedDict
from datetime import date, timedelta
from sqlalchemy import func, or_, and_, column, not_
from app import db
from app.api.forecast.models import PortfolioInfo, PortfolioData, PortfolioValueDaily, PortfolioCompareInfo, \
    PortfolioCompareResult
import pandas as pd
from app.utils.fh_utils import str_2_date, date_2_str
# from app.utils.db_utils import with_db_session
from app.config import config
import logging
import json

logger = logging.getLogger()
ASSET_TYPE_TABLE_NAME_DIC = {
    'stock': (config.BIND_DB_NAME_MD, 'wind_stock_daily'),
    'index': (config.BIND_DB_NAME_MD, 'wind_index_daily'),
    'future': (config.BIND_DB_NAME_MD, 'wind_future_daily'),
    'portfolio': (None, 'pl_value_daily'),
    'pl': (None, 'pl_value_daily'),  # 部分历史数据使用简写 pl
    'value': (None, None),
}


def update_pl_value_daily(pl_id, date_from, date_to, name='')->pd.DataFrame:
    """
    更新指定日期范围内组合每日收益率
    :param pl_id:
    :param date_from:
    :param date_to:
    :param name:
    :return:
    """
    # logger.debug('kwargs: %s', kwargs)
    rr_tot_df = None
    date_from = str_2_date(date_from)
    date_to = str_2_date(date_to)
    # TODO 根据pl_info 指定的计算方法进行每日投资组合计算，目前默认统计的是每日的收益率
    # pl_info = db.session.query(PortfolioInfo).get(pl_id)
    # 根据pl日期区间与函数日期区间参数 取交集获取日期区间数据
    sql_str = """SELECT dt_to.pl_id, ifnull( dt_frm.date_frm, date(%s)) date_from, dt_to.date_to
FROM
(
    SELECT pl_id, max(trade_date) date_to
    FROM pl_data
    WHERE pl_id = %s AND trade_date <= %s
) dt_to
LEFT JOIN
(
    SELECT pl_id,max(trade_date) date_frm
    FROM pl_data
    WHERE pl_id = %s AND trade_date <= %s
) dt_frm
ON dt_frm.pl_id = dt_to.pl_id"""
    trade_date_range_df = pd.read_sql(sql_str, db.engine,
                                      params=[date_from, pl_id, date_to, pl_id, date_from])
    if trade_date_range_df.shape[0] == 0:
        logger.debug('pl_id=%d %s [%s %s] 没有数据',
                     pl_id, name, date_from, date_to)
        return rr_tot_df
    elif trade_date_range_df.shape[0] > 1:
        logger.warning('pl_id=%d %s [%s %s] 存在 %d 条记录',
                       pl_id, name, date_from, date_to, trade_date_range_df.shape[0])
        return rr_tot_df

    # 获取日期段内的全部组合
    date_range_to = trade_date_range_df['date_to'][0]
    date_range_from = trade_date_range_df['date_from'][0]
    sql_str = """SELECT id, pl_id, asset_code, trade_date, weight, asset_type FROM pl_data
    WHERE pl_id = %s AND trade_date BETWEEN %s AND %s"""
    pl_data_df_all = pd.read_sql(sql_str, db.engine, params=[pl_id, date_range_from, date_range_to])
    if pl_data_df_all.shape[0] == 0:
        logger.warning('pl_id=%d %s [%s %s] 没有有效的数据[%s - %s]',
                       pl_id, name, date_from, date_to, date_range_to, date_range_from)
        return rr_tot_df
    dfg = pl_data_df_all.groupby('trade_date')
    trade_date_sorted_dic = OrderedDict()
    trade_date_sorted_list = []
    for trade_date_g, pl_data_df in dfg:
        trade_date_sorted_list.append(trade_date_g)
        trade_date_sorted_dic[trade_date_g] = {
            'pl_df': pl_data_df[["asset_code", "weight", "asset_type"]].set_index('asset_code')}

    # 计算各个时间点的组合对应的日期范围
    if len(trade_date_sorted_list) == 1:
        trade_date_from_cur = trade_date_sorted_list[0]
        trade_date_range_frm = date_from if trade_date_from_cur < date_from else trade_date_from_cur
        if trade_date_range_frm > date_to:
            logger.warning('pl_id=%d %s [%s %s] 日期范围 [%s - %s] 无效',
                           pl_id, name, date_from, date_to, trade_date_range_frm, date_to)
            return rr_tot_df
        trade_date_sorted_dic[trade_date_from_cur]['date_from'] = trade_date_range_frm
        trade_date_sorted_dic[trade_date_from_cur]['date_to'] = date_to
    else:
        # 按日期从小到大排序
        trade_date_sorted_list.sort()
        trade_date_sorted_count = len(trade_date_sorted_list)
        for idx in range(0, trade_date_sorted_count):
            # from
            trade_date_from_cur = trade_date_sorted_list[idx]
            trade_date_range_frm = date_from if date_from > trade_date_from_cur else trade_date_from_cur
            # to
            if idx < trade_date_sorted_count - 1:
                trade_date_from_next = trade_date_sorted_list[idx + 1]
                trade_date_range_to = trade_date_from_next - timedelta(days=1)
            else:
                trade_date_range_to = date_to
            # 检查有效性
            if trade_date_range_frm > trade_date_range_to:
                logger.debug('pl_id=%d %s [%s %s] %d 日期范围 [%s - %s] 无效',
                             pl_id, name, date_from, date_to, idx, trade_date_range_frm, trade_date_range_to)
                continue
            trade_date_sorted_dic[trade_date_from_cur]['date_from'] = trade_date_range_frm
            trade_date_sorted_dic[trade_date_from_cur]['date_to'] = trade_date_range_to

    # 分阶段计算收益率
    rr_s_list = []
    for num, (trade_date_g, data_dic) in enumerate(trade_date_sorted_dic.items()):
        if 'date_from' in data_dic:
            rr_s = calc_portfolio_rr(**data_dic)
            if rr_s is not None and rr_s.shape[0] > 0:
                rr_s_list.append(rr_s)
    if len(rr_s_list) == 0:
        logger.warning('pl_id=%d %s [%s %s] 日期范围 没有 rr 结果',
                       pl_id, name, date_from, date_to)
        return rr_tot_df
    # 合并计算结果
    rr_tot_s = pd.concat(rr_s_list).rename('rr')
    rr_tot_df = pd.DataFrame(rr_tot_s)
    rr_tot_df['pl_id'] = pl_id
    # 更新数据库
    # 删除数据库中历史数据
    # with with_db_session(db.engine) as session:
    #     session.execute(
    #         'DELETE FROM pl_value_daily WHERE trade_date BETWEEN :date_from AND :date_to AND pl_id = :pl_id',
    #         params={"date_from": date_2_str(rr_tot_s.index.min()),
    #                 "date_to": date_2_str(rr_tot_s.index.max()),
    #                 'pl_id': pl_id}
    #     )
    #     session.commit()
    PortfolioValueDaily.query.filter(
        PortfolioValueDaily.pl_id == pl_id,
        PortfolioValueDaily.trade_date >= rr_tot_s.index.min()
    ).delete()
    db.session.commit()
    # 获取最新净值
    nav_latest = db.session.query(PortfolioValueDaily.nav).filter(
        PortfolioValueDaily.pl_id == pl_id,
        PortfolioValueDaily.trade_date == (
            db.session.query(func.max(PortfolioValueDaily.trade_date)).filter(
                PortfolioValueDaily.pl_id == pl_id)
        )
    ).scalar()
    # 计算此后日净值
    # logger.info('nav_latest=%f', nav_latest)
    rr_tot_df['nav'] = (rr_tot_df['rr'] + 1).cumprod() * (float(nav_latest) if nav_latest is not None else 1)
    rr_tot_df.to_sql("pl_value_daily", db.engine, if_exists='append')
    return rr_tot_df


def get_trade_date_range_df(date_from, date_to, pl_df: pd.DataFrame):
    """
    获取每一个 portfolio 的交易日起止日期，日期段前一个交易日(date_from - 1)，到最后一个交易日(date_to)
    :param date_from:
    :param date_to:
    :param pl_df:wind_code 为index, weight 列为权重, price_type 价格类型, asset_type 代表资产类别 stock\index\future...
    :return:
    """
    trade_date_range_df = None
    if pl_df is None or pl_df.shape[0] == 0:
        logger.warning('[%s - %s]: 没有数据', date_from, date_to)
        return trade_date_range_df
    # 对 weight 进行 normalize
    pl_df['weight'] = pl_df['weight'] / pl_df['weight'].sum()
    pl_dfg = pl_df.groupby('asset_type')
    trade_date_range_df_list = []
    # 股票数据
    asset_type = 'stock'
    for asset_type, pl_sub_df in pl_dfg:
        if asset_type not in ASSET_TYPE_TABLE_NAME_DIC:
            logger.warning('[%s - %s] asset_type = %s 无效', date_from, date_to, asset_type)
            continue
        bind_db, table_name = ASSET_TYPE_TABLE_NAME_DIC[asset_type]
        pl_sub_df = pl_dfg.get_group(asset_type)
        # 构建 in 子句中的字符串，将 wind_code 连接起来
        in_list_str = "'" + "','".join(pl_sub_df.index) + "'"

        # 构筑sql 获取每只股票日期段前一个交易日(date_from - 1)，到最后一个交易日(date_to)
        # select dt_to.wind_code, ifnull( dt_frm.date_frm, date('2018-01-01')) date_frm, dt_to.date_to
        # from
        # (
        #     SELECT wind_code, max(trade_date) date_to
        #     FROM wind_stock_daily
        #     WHERE trade_date <= '2018-05-01'
        #     and wind_code in ('000001.SZ', '000002.SZ')
        #     group by wind_code
        # ) dt_to
        # left JOIN
        # (
        #     select wind_code,max(trade_date) date_frm
        #     FROM wind_stock_daily
        #     WHERE trade_date < '2018-01-01'
        #     and wind_code in ('000001.SZ', '000002.SZ')
        #     group by wind_code
        # ) dt_frm
        # on dt_frm.wind_code = dt_to.wind_code
        sql_str = """select dt_to.wind_code asset_code, ifnull( dt_frm.date_frm, date(%s)) date_from, dt_to.date_to
        from
        (
            SELECT wind_code, max(trade_date) date_to
            FROM {1}
            WHERE trade_date <= %s
            and wind_code in ({0})
            group by wind_code
        ) dt_to
        left JOIN
        (
            select wind_code,max(trade_date) date_frm
            FROM {1}
            WHERE trade_date < %s  -- 需要找到前一个交易日的日期，因此是 小于号
            and wind_code in ({0})
            group by wind_code
        ) dt_frm
        on dt_frm.wind_code = dt_to.wind_code""".format(in_list_str, table_name)
        # 获取日期区间
        trade_date_range_sub_df = pd.read_sql(
            sql_str,
            db.get_engine(bind=bind_db) if bind_db is not None else db.engine,
            params=[date_from, date_to, date_from])
        if trade_date_range_sub_df.shape[0] > 0:
            trade_date_range_sub_df["asset_type"] = asset_type
            trade_date_range_df_list.append(trade_date_range_sub_df)
        else:
            logger.debug('[%s %s] asset_type %s [%s] 没有数据', date_from, date_to, asset_type, in_list_str)

    # 整合数据
    if len(trade_date_range_df_list) > 0:
        trade_date_range_df = pd.concat(trade_date_range_df_list)

    return trade_date_range_df


def calc_portfolio_rr(date_from, date_to, pl_df: pd.DataFrame):
    """
    计算日期区间投资组合收益率
    :param date_from:
    :param date_to:
    :param pl_df: wind_code 为index, weight 列为权重, asset_type 代表资产类别 stock\index\future...
    :return:
    """
    if pl_df is None or pl_df.shape[0] == 0:
        logger.warning('pl_df [%s - %s]: 没有数据', date_from, date_to)
        return
    trade_date_range_df = get_trade_date_range_df(date_from, date_to, pl_df)

    if trade_date_range_df is None or trade_date_range_df.shape[0] == 0:
        logger.debug('[%s %s] %s 没有数据', date_from, date_to, list(pl_df['asset_code']))
        return

    # 分别计算每日收益率
    rr_list = []
    for idx, trade_date_range_dic in trade_date_range_df.to_dict('index').items():
        asset_code = trade_date_range_dic["asset_code"]
        rr_weighted_s = calc_asset_rr(**trade_date_range_dic) * pl_df.loc[asset_code, 'weight']
        rr_list.append(rr_weighted_s)
    # 整理成df index为日期 column 为股票代码
    rr_weighted_df = pd.DataFrame(rr_list).T
    rr_s = rr_weighted_df.sum(axis=1)
    # 对日期字段进行过滤
    is_fit = (rr_s.index >= date_from) & (rr_s.index <= date_to)
    ret_s = rr_s[is_fit]
    return ret_s


def calc_asset_rr(asset_code, date_from, date_to, asset_type) -> pd.Series:
    """
    计算资产日期区间的收益率
    :param asset_code:
    :param date_from:
    :param date_to:
    :param asset_type:
    :return:
    """
    # if asset_type == 'stock':
    #     table_name = 'wind_stock_daily'
    # elif asset_type == 'index':
    #     table_name = 'wind_index_daily'
    # elif asset_type == 'future':
    #     table_name = 'wind_future_daily'
    # else:
    #     raise ValueError("asset_type:%s 无效" % asset_type)
    bind_db, table_name = ASSET_TYPE_TABLE_NAME_DIC[asset_type]
    # TODO: 增加对 portfolio, value 两类资产的处理
    sql_str = """select trade_date, close from {table_name} where wind_code=%s and trade_date between %s and %s order by trade_date""".format(
        table_name=table_name)
    md_df = pd.read_sql(sql_str,
                        db.get_engine(bind=bind_db) if bind_db is not None else db.engine,
                        params=[asset_code, date_from, date_to],
                        index_col='trade_date')
    rr_s = md_df["close"].pct_change().fillna(0).rename(asset_code)
    return rr_s


def update_pl_all(force_update_since_trade_date=None):
    """
    定期执行，检查全部 pl 列表，更新 pl_value_daily 数据
    :param force_update_since_trade_date:
    :return:
    """
    # 获取 pl_compare_info 列表有效的日期范围
    if force_update_since_trade_date is None:
        sql_str = """SELECT pl_id, name, date_from, date_to
FROM
(
    SELECT pl_info.pl_id, name
      , if(trade_date_max IS NULL, date_from, adddate(trade_date_max, INTERVAL 1 DAY)) date_from
      , date_to
    FROM
        pl_info
    LEFT JOIN
        (
        SELECT pl_id, max(trade_date) trade_date_max FROM pl_value_daily GROUP BY pl_id
        ) rst
    ON pl_info.pl_id = rst.pl_id
    WHERE is_del = 0
) date_range
WHERE date_from <= date_to"""
        # 查询数据库
        date_range_df = pd.read_sql(sql_str, db.engine)
    else:
        sql_str = """SELECT pl_id, name, if(date_from < date(%s), date_from, date(%s)) date_from, date_to
FROM
(
    SELECT pl_info.pl_id, name
      , if(trade_date_max IS NULL, date_from, adddate(trade_date_max, INTERVAL 1 DAY)) date_from
      , date_to
    FROM
        pl_info
    LEFT JOIN
        (
        SELECT pl_id, max(trade_date) trade_date_max FROM pl_value_daily GROUP BY pl_id
        ) rst
    ON pl_info.pl_id = rst.pl_id
    WHERE is_del = 0
) date_range
WHERE date_from <= date_to AND date_to >= %s"""
        # 查询数据库
        date_range_df = pd.read_sql(sql_str, db.engine,
                                    params=[force_update_since_trade_date,
                                            force_update_since_trade_date,
                                            force_update_since_trade_date])
    # 更新数据
    data_count = date_range_df.shape[0]
    if data_count > 0:
        for data_num, date_range_dic in date_range_df.to_dict(orient='index').items():
            logger.debug('%d/%d) 更新 pl_id=%d %s [%s %s]',
                         data_num + 1, data_count, date_range_dic['pl_id'], date_range_dic['name'],
                         date_range_dic['date_from'], date_range_dic['date_to'])
            rr_tot_df = update_pl_value_daily(**date_range_dic)
            if rr_tot_df is not None:
                date_max = max(rr_tot_df.index)
                logger.debug('%d/%d) 更新 pl_id=%d %s [%s %s] 到最新日期 %s 更新 %d 条记录',
                             data_num + 1, data_count, date_range_dic['pl_id'], date_range_dic['name'],
                             date_range_dic['date_from'], date_range_dic['date_to'],
                             date_max, rr_tot_df.shape[0])
                # 更新状态
                if date_range_dic['date_to'] is not None and str_2_date(date_range_dic['date_to']) <= date_max:
                    PortfolioValueDaily.query.filter(
                        PortfolioValueDaily.pl_id == date_range_dic['pl_id']
                    ).update({'status': 'complete'})


def update_pl_compare_result(cmp_id, date_from, date_to, **kwargs):
    """
    对给定 cmp_id 更新指定日期段内的比较结果
    PortfolioCompareInfo 中 params 格式
    compare_method：'>', '<', 'between'
    compare_method 为 between时，asset1 与两个参考数据进行比较，否则与1个参考数据进行比较
    compare_type：'rel.rr', 'abs.rr', 'abs.fix_point'
    compare_type 为 rel 与 资产或资产组合进行比较，abs 与数值进行比较；
    rr 代表与收益率进行比较，fix_point 代表与实际价格进行比较
    :param cmp_id:
    :param date_from:
    :param date_to:
    :param kwargs: 便于被调用时接受其他冗余参数使用，暂时无用
    :return:
    """
    result_df = None
    pl_comp_info = PortfolioCompareInfo.query.get(cmp_id)
    if pl_comp_info.params is None or pl_comp_info.params == "":
        raise ValueError('cmp_id: %s 无效' % cmp_id)
    # 获取参数，计算日期范围交集
    date_from = max([pl_comp_info.date_from, str_2_date(date_from)])
    date_to = min([pl_comp_info.date_to, str_2_date(date_to)])
    param_dic = json.loads(pl_comp_info.params)
    if 'date_start' in param_dic and len(param_dic['date_start']) >= 6:
        date_start = max([str_2_date(param_dic['date_start']), date_from])
    else:
        date_start = max([pl_comp_info.date_from, date_from])

    compare_method = param_dic['compare_method']
    if compare_method not in ('>', '<', 'between'):
        raise ValueError("cmp_id: %s param_dic['compare_method']: %s 无效" %
                         (cmp_id, param_dic['compare_method']))
    compare_type = param_dic['compare_type']
    if compare_type not in ('rel.rr', 'abs.rr', 'abs.fix_point'):
        raise ValueError("cmp_id: %s param_dic['compare_type']: %s 无效" %
                         (cmp_id, param_dic['compare_type']))
    has_value_asset = False  # 判断是否存在 value 类型的资产
    max_len_asset_n = None
    val_s_dic = {}
    need_repead_dic = {}
    # 获取相关数据，每条数据为一组 Series, 合并组成 df
    for asset_num in range(1, 4 if compare_method == 'between' else 3):  # 以后可能出现不止2个数值进行运算

        asset_type_n_key = 'asset_type_' + str(asset_num)
        asset_type = param_dic[asset_type_n_key]
        # 用来从 param_dic 中取出相应的参数
        asset_or_value_n_key = 'value_' + str(asset_num) if asset_type == 'value' else 'asset_' + str(asset_num)
        # 用来保持到计算结果 Series 中并对列进行命名，后续的比较计算均依赖于此名称
        asset_n_key = 'asset_' + str(asset_num)
        bind_db, table_name = ASSET_TYPE_TABLE_NAME_DIC[asset_type]

        if table_name is None:
            has_value_asset = True
            val_s = pd.Series(name=asset_n_key, data=param_dic[asset_or_value_n_key])
            need_repeat = True

        elif table_name == 'pl_value_daily':
            engine = db.get_engine(bind=bind_db) if bind_db is not None else db.engine
            sql_str = """SELECT trade_date, rr FROM pl_value_daily 
            WHERE pl_id = %s AND trade_date BETWEEN %s AND %s 
            ORDER BY trade_date"""
            val_s = pd.read_sql(sql_str, engine,
                                params=[param_dic[asset_or_value_n_key], date_start, date_to],
                                index_col='trade_date'
                                )['rr'].rename(asset_n_key)
            val_s = (val_s + 1).cumprod()
            need_repeat = False
        else:
            engine = db.get_engine(bind=bind_db) if bind_db is not None else db.engine
            sql_str = """select trade_date, close from {0} 
            where wind_code = %s and trade_date between %s and %s 
            ORDER BY trade_date""".format(table_name)
            val_s = pd.read_sql(sql_str, engine,
                                params=[param_dic[asset_or_value_n_key], date_start, date_to],
                                index_col='trade_date'
                                )['close'].rename(asset_n_key)
            need_repeat = False

        # 加入字典
        val_s_dic[asset_n_key] = val_s
        need_repead_dic[asset_n_key] = need_repeat
        # 选择最长的一组数据进行记录，用于最后将 value型数据扩展长度
        if not need_repeat:
            if max_len_asset_n is None:
                max_len_asset_n = asset_n_key
            elif len(val_s_dic[max_len_asset_n]) < len(val_s):
                max_len_asset_n = asset_n_key

    # 如果存在 value 型数据则扩展期长度与最长的一组数据一致
    if has_value_asset:
        for asset_n_key, need_repeat in need_repead_dic.items():
            if need_repeat:
                max_len_asset_val_s = val_s_dic[max_len_asset_n]
                asset_val = val_s_dic[asset_n_key].repeat(len(max_len_asset_val_s))
                asset_val.index = max_len_asset_val_s.index
                val_s_dic[asset_n_key] = asset_val

    # 合并数据
    data_df = pd.DataFrame(val_s_dic)
    # 只比较 pl_comp_info.date_from  pl_comp_info.date_to 之间的数据
    is_fit = (data_df.index >= pl_comp_info.date_from) & (data_df.index <= pl_comp_info.date_to)
    data_sub_df = data_df[is_fit]
    if data_sub_df.shape[0] == 0:
        return result_df
    if compare_method == '>':
        func = compare_func_larger
    elif compare_method == '<':
        func = compare_func_larger
    elif compare_method == 'between':
        func = compare_func_between
    else:
        raise ValueError("cmp_id: %s param_dic['compare_method']: %s 暂不支持" %
                         (cmp_id, param_dic['compare_method']))

    calc_result_df = pd.DataFrame(
        [pd.Series(func(y), name=x, index=['result', 'shift_value', 'shift_rate'])
         for x, y in data_sub_df.T.items()], index=data_sub_df.index)
    result_df = data_sub_df.merge(calc_result_df, left_index=True, right_index=True)
    if result_df.shape[0] == 0:
        return None
    # data_sub_df['result'] = data_sub_df.apply(func, axis=1)
    result_df['cmp_id'] = cmp_id
    result_df.index.rename('trade_date', inplace=True)
    # 删除重复数据
    result_date_min = result_df.index.min()
    result_date_max = result_df.index.max()
    db.session.query(PortfolioCompareResult).filter(
        PortfolioCompareResult.cmp_id == cmp_id,
        PortfolioCompareResult.trade_date >= result_date_min,
        PortfolioCompareResult.trade_date <= result_date_max).delete()
    db.session.commit()
    # 插入数据
    result_df.to_sql('pl_compare_result', db.engine, if_exists='append')
    logger.debug('更新 cmp_id:%d %s - %s %d 条记录',
                 cmp_id, date_2_str(result_date_min), date_2_str(result_date_max), result_df.shape[0])
    return result_date_max


def compare_func_larger(val_s):
    """仅供 update_compare_pl 中比较函数使用"""
    result = val_s['asset_1'] > val_s['asset_2']
    shift_value = val_s['asset_2'] - val_s['asset_1']
    shift_rate = shift_value / val_s['asset_1']
    return [result, shift_value, shift_rate]


def compare_func_smaller(val_s):
    """仅供 update_compare_pl 中比较函数使用"""
    result = val_s['asset_1'] < val_s['asset_2']
    shift_value = val_s['asset_2'] - val_s['asset_1']
    shift_rate = shift_value / val_s['asset_1']
    return [result, shift_value, shift_rate]


def compare_func_between(val_s):
    """仅供 update_compare_pl 中比较函数使用"""
    result = val_s['asset_2'] <= val_s['asset_1'] <= val_s['asset_3']
    shift_value = val_s['asset_2'] - val_s['asset_1']
    shift_rate = shift_value / val_s['asset_1']
    return [result, shift_value, shift_rate]


def update_compare_pl_all(force_update_since_trade_date=None):
    """
    定期执行，检查全部比较列表，更新数据库
    :param force_update_since_trade_date:
    :return:
    """
    # 获取 pl_compare_info 列表有效的日期范围
    if force_update_since_trade_date is None:
        sql_str = """SELECT cmp_id, name, date_from, date_to
FROM
(
    SELECT cmp_info.cmp_id, name, if(trade_date_max IS NULL
    , date_from, adddate(trade_date_max, INTERVAL 1 DAY)) date_from, date_to
    FROM 
        pl_compare_info cmp_info
    LEFT JOIN
        (
        SELECT cmp_id, max(trade_date) trade_date_max FROM pl_compare_result GROUP BY cmp_id
        ) rst
    ON cmp_info.cmp_id = rst.cmp_id
    WHERE is_del = 0
) date_range
WHERE date_from <= date_to"""
        # 查询数据库
        cmp_date_range_df = pd.read_sql(sql_str, db.engine)
    else:
        sql_str = """SELECT * FROM (
    SELECT cmp_id, name, if(date_range.date_from < %s, date_range.date_from, date(%s)) date_from, date_to
    FROM
    (
        SELECT cmp_info.cmp_id, name, 
          if(trade_date_max IS NULL, date_from, adddate(trade_date_max, INTERVAL 1 DAY)) date_from, date_to
        FROM 
            pl_compare_info cmp_info
        LEFT JOIN
            (
            SELECT cmp_id, max(trade_date) trade_date_max FROM pl_compare_result GROUP BY cmp_id
            ) rst
        ON cmp_info.cmp_id = rst.cmp_id
        WHERE is_del = 0
    ) date_range
    ) t1
    WHERE date_from <= date_to AND date_to >= %s"""
        # 查询数据库
        cmp_date_range_df = pd.read_sql(
            sql_str, db.engine,
            params=[force_update_since_trade_date, force_update_since_trade_date, force_update_since_trade_date])
    # 更新数据
    data_count = cmp_date_range_df.shape[0]
    if data_count > 0:
        for data_num, cmp_data_range in cmp_date_range_df.to_dict(orient='index').items():
            logger.debug('%d/%d) 更新 %d %s [%s  %s]',
                         data_num + 1, data_count, cmp_data_range['cmp_id'], cmp_data_range['name'],
                         cmp_data_range['date_from'], cmp_data_range['date_to'])
            try:
                date_max = update_pl_compare_result(**cmp_data_range)
            except Exception as exp:
                logger.exception('执行 update_pl_compare_result(%s) 错误', cmp_data_range)
                date_max = None
            if date_max is not None:
                logger.debug('%d/%d) 更新 %d %s [%s  %s] 到最新日期 %s',
                             data_num + 1, data_count, cmp_data_range['cmp_id'], cmp_data_range['name'],
                             cmp_data_range['date_from'], cmp_data_range['date_to'],
                             date_max)
                # 更新状态
                if cmp_data_range['date_to'] is not None and str_2_date(cmp_data_range['date_to']) <= date_max:
                    PortfolioCompareInfo.query.filter(
                        PortfolioCompareInfo.cmp_id == cmp_data_range['cmp_id']
                    ).update({'status': 'complete'})


if __name__ == "__main__":
    from app.app import app

    # 测试 更新指定日期区间的投资组合计算结果
    # pl_id, date_from, date_to = 2, '2018-03-8', '2018-5-1'
    # update_pl_value_daily(pl_id, date_from, date_to)

    # 测试 更新全部投资组合历史收益率
    force_update_since_trade_date = '2000-01-01'
    update_pl_all(force_update_since_trade_date=force_update_since_trade_date)

    # 测试 更新指定日期区间的预测结果
    # cmp_id, trade_date_from, trade_date_to = 11, '2000-01-01', '2018-5-1'
    # update_pl_compare_result(cmp_id, trade_date_from, trade_date_to)

    # 测试 更新全部比较列表
    # trade_date = '2018-01-01'
    # update_compare_pl_all(trade_date)
