# -*- coding:utf-8 -*-
####################################################################################################################
'''
DB create query
use invest ;
drop TABLE IF EXISTS stock_daily_data ;

CREATE TABLE IF NOT EXISTS stock_daily_data (
  index1 bigint(20) NOT NULL,
  trade_date date NOT NULL COMMENT '交易日期',
  stock_code varchar(100) NOT NULL COMMENT '股票代码',
  open double DEFAUstock_daily_dataLT NULL COMMENT '开盘价',
  high double DEFAULT NULL COMMENT '最高价',
  low double DEFAULT NULL COMMENT '最低价',
  close double DEFAULT NULL COMMENT ' 收盘价,证券在交易日所在指定周期的最后一条行情数据中的收盘价',
  ev double DEFAULT NULL COMMENT '上市公司的股权公平市场价值。对于一家多地上市公司，区分不同类型的股份价格和股份数量分别计算类别市值，然后加总',
  mkt_cap_ard double DEFAULT NULL COMMENT '按指定证券价格乘指定日总股本计算上市公司在该市场的估值。该总市值为计算PE、PB等估值指标的基础指标。暂停上市期间或退市后该指标不计算。',
  val_pe_deducted_ttm double DEFAULT NULL COMMENT '扣非后的市盈率(TTM)=总市值/前推12个月扣除非经常性损益后的净利润',
  pe_lyr double DEFAULT NULL COMMENT ' 每股股价为每股收益的倍数。可回测的估值指标。  总市值2/归属母公司股东净利润（LYR) 注： 1、总市值2=指定日证券收盘价*指定日当日总股本2、B股涉及汇率转换',
  pb_lf double DEFAULT NULL COMMENT '普通股每股市价为每股净资产的倍数。 总市值2／指定日最新公告股东权益(不含少数股东权益)注： 1、总市值2=指定日证券收盘价*指定日当日总股本2、B股涉及汇率转换',
  created_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建日期',
  updated_date datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更新日期',
  UNIQUE KEY idx_code_date (stock_code,trade_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

ALTER TABLE `invest`.`stock_daily_data` 
CHANGE COLUMN `index1` `index` BIGINT(20) NOT NULL ;
'''
####################################################################################################################
import pandas as pd
from WindPy import *
from sqlalchemy import create_engine
import datetime,time
import os
import pymysql

class WindStock():

    def getCurrentTime(self):
        # 获取当前时间
        return time.strftime('[%Y-%m-%d %H:%M:%S]', time.localtime(time.time()))

    def AStockHisData(self,symbols,start_date,end_date,step=0):
        '''
        逐个股票代码查询行情数据
        wsd代码可以借助 WindNavigator自动生成copy即可使用;时间参数不设，默认取当前日期，可能是非交易日没数据;
        只有一个时间参数时，默认作为为起始时间，结束时间默认为当前日期；如设置两个时间参数则依次为起止时间
        '''
        print(self.getCurrentTime(),": Download A Stock Starting:")
        for symbol in symbols:
             w.start()
             try:
                 #stock=w.wsd(symbol,'trade_code,open,high,low,close,volume,amt',start_date,end_date)
                 '''
                 wsd代码可以借助 WindNavigator自动生成copy即可使用;
                 时间参数不设，默认取当前日期，可能是非交易日没数据;
                 只有一个时间参数，默认为起始时间到最新；如设置两个时间参数则依次为起止时间
                '''
                 #stock=w.wsd(symbol, "trade_code,open,high,low,close,pre_close,volume,amt,dealnum,chg,pct_chg,vwap, adjfactor,close2,turn,free_turn,oi,oi_chg,pre_settle,settle,chg_settlement,pct_chg_settlement, lastradeday_s,last_trade_day,rel_ipo_chg,rel_ipo_pct_chg,susp_reason,close3, pe_ttm,val_pe_deducted_ttm,pe_lyr,pb_lf,ps_ttm,ps_lyr,dividendyield2,ev,mkt_cap_ard,pb_mrq,pcf_ocf_ttm,pcf_ncf_ttm,pcf_ocflyr,pcf_nflyr,trade_status", start_date,end_date)
                 stock=w.wsd(symbol,'trade_code,open,high,low,close,ev,mkt_cap_ard,val_pe_deducted_ttm,pe_lyr,pb_lf', start_date,end_date)
                 index_data = pd.DataFrame()
                 index_data['trade_date']=stock.Times
                 stock.Data[0]=symbol
                 index_data['stock_code']=stock.Data[0]
                 #index_data['stock_code'] =symbol
                 index_data['open'] =stock.Data[1]
                 index_data['high'] =stock.Data[2]
                 index_data['low']  =stock.Data[3]
                 index_data['close']=stock.Data[4]
#                 index_data['pre_close']=stock.Data[5]
#                 index_data['volume']=stock.Data[6]
#                 index_data['amt']=stock.Data[7]
#                 index_data['dealnum']=stock.Data[8]
#                 index_data['chg']=stock.Data[9]
#                 index_data['pct_chg']=stock.Data[10]
#                 #index_data['pct_chg']=index_data['pct_chg']/100
#                 index_data['vwap']=stock.Data[11]
#                 index_data['adj_factor']=stock.Data[12]
#                 index_data['close2']=stock.Data[13]
#                 index_data['turn']=stock.Data[14]
#                 index_data['free_turn']=stock.Data[15]
#                 index_data['oi']=stock.Data[16]
#                 index_data['oi_chg']=stock.Data[17]
#                 index_data['pre_settle']=stock.Data[18]
#                 index_data['settle']=stock.Data[19]
#                 index_data['chg_settlement']=stock.Data[20]
#                 index_data['pct_chg_settlement']=stock.Data[21]
#                 index_data['lastradeday_s']=stock.Data[22]
#                 index_data['last_trade_day']=stock.Data[23]
#                 index_data['rel_ipo_chg']=stock.Data[24]
#                 index_data['rel_ipo_pct_chg']=stock.Data[25]
#                 index_data['susp_reason']=stock.Data[26]
#                 index_data['close3']=stock.Data[27]
#                 index_data['pe_ttm']=stock.Data[28]
                 index_data['val_pe_deducted_ttm']=stock.Data[7]
                 index_data['pe_lyr']=stock.Data[8]
                 index_data['pb_lf']=stock.Data[9]
#                 index_data['ps_ttm']=stock.Data[32]
#                 index_data['ps_lyr']=stock.Data[33]
#                 index_data['dividendyield2']=stock.Data[34]
                 index_data['ev']=stock.Data[5]
                 index_data['mkt_cap_ard']=stock.Data[6]
#                 index_data['pb_mrq']=stock.Data[37]
#                 index_data['pcf_ocf_ttm']=stock.Data[38]
#                 index_data['pcf_ncf_ttm']=stock.Data[39]
#                 index_data['pcf_ocflyr']=stock.Data[40]
#                 index_data['pcf_ncflyr']=stock.Data[41]
#                 index_data['trade_status']=stock.Data[42]
#                 index_data['data_source']='Wind'
                 index_data['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                 index_data['updated_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                 index_data = index_data[index_data['open'] > 0]
                 #index_data.fillna(0)
                 try:
                    index_data.to_sql('stock_daily_data',engine,if_exists='append');
                 except Exception as e:
                     error_log=pd.DataFrame()
                     error_log['trade_date']=stock.Times
                     error_log['stock_code']=stock.Data[0]
                     error_log['start_date']=start_date
                     error_log['end_date']=end_date
                     error_log['status']=None
                     error_log['table']='stock_daily_data'
                     error_log['args']='Symbol: '+symbol+' From '+start_date+' To '+end_date
                     error_log['error_info']=e
                     error_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                     error_log.to_sql('stock_error_log',engine,if_exists='append')
                     print ( self.getCurrentTime(),": SQL Exception :%s" % (e) )
                     continue
                 w.start()
             except Exception as e:
                     #如果读取处理失败，可能是网络中断、频繁访问被限、历史数据缺失等原因。写入相关信息到日志表，便于后续补充处理
                     error_log=pd.DataFrame()
                     error_log['trade_date']=stock.Times
                     error_log['stock_code']=stock.Data[0]
                     error_log['start_date']=start_date
                     error_log['end_date']=end_date
                     error_log['status']=None
                     error_log['table']='stock_daily_data'
                     error_log['args']='Symbol: '+symbol+' From '+start_date+' To '+end_date
                     error_log['error_info']=e
                     error_log['created_date']=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                     error_log.to_sql('stock_error_log',engine,if_exists='append')
                     print ( self.getCurrentTime(),":index_data %s : Exception :%s" % (symbol,e) )
                     time.sleep(sleep_time)
                     w.start()
                     continue
             print(self.getCurrentTime(),": Downloading [",symbol,"] From "+start_date+" to "+end_date)
        print(self.getCurrentTime(),": Download A Stock Has Finished .")


def main():

    global engine,sleep_time,symbols
    sleep_time=20
    windStock=WindStock()
    engine = create_engine('mysql+mysqlconnector://root:123456@localhost/invest?charset=utf8')
    start_date='20171211'
    end_date='20181211'
    #symbols=windStock.getAStockCodesFromCsv()#通过文件获取股票代码
    #symbols=windStock.getAStockCodesWind(end_date)#通过Wind API获取股票代码,默认取最新的，可以指定取历史某一日所有A股代码
    symbols=['600887.SH']#通过直接赋值获取股票代码用于测试
    print (symbols)
    windStock.AStockHisData(symbols,start_date,end_date)

#def test():
#    '''
#    测试脚本，个人账户只能获取最近100天数据
#    '''
#    symbol='000001.SZ'
#    start_date='20170101'
#    end_date='20170109'
#    w.start();
#    stock=w.wsd(symbol,'trade_code,open,high,low,close')
#    #stock=w.wsd(symbol, "trade_status,open,high,low,close,pre_close,volume,amt,dealnum,chg,pct_chg,vwap, adjfactor,close2,turn,free_turn,oi,oi_chg,pre_settle,settle,chg_settlement,pct_chg_settlement, lastradeday_s,last_trade_day,rel_ipo_chg,rel_ipo_pct_chg,susp_reason,close3, pe_ttm,val_pe_deducted_ttm,pe_lyr,pb_lf,ps_ttm,ps_lyr,dividendyield2,ev,mkt_cap_ard,pb_mrq,pcf_ocf_ttm,pcf_ncf_ttm,pcf_ocflyr,pcf_nflyr", start_date,end_date)
#    #stock=w.wsd("000001.SZ", "pre_close,open,high,low,close,volume,amt,dealnum,chg,pct_chg,vwap,adjfactor,close2,turn,free_turn,oi,oi_chg,pre_settle,settle,chg_settlement,pct_chg_settlement,lastradeday_s,last_trade_day,rel_ipo_chg,rel_ipo_pct_chg,trade_status,susp_reason,close3", "2016-12-09", "2017-01-07", "adjDate=0")
#    print (stock)

if __name__ == "__main__":
    main()

