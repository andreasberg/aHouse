# import Jinja2 template engine
from jinja2 import Environment, FileSystemLoader

# import Tornado
from tornado.ioloop import IOLoop
from tornado import gen
from tornado import web
from tornado.httpclient import AsyncHTTPClient,HTTPRequest,HTTPError
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
import tornado.escape

# DB
import sqlite3
from tornado_mysql import pools
import MySQLdb.cursors
#import pymysql.cursors
import pandas as pd
import numpy as np
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy import Table
from sqlalchemy import create_engine


# Date and time
from datetime import datetime,timezone
import pytz
from tzlocal import get_localzone
import time
from apscheduler.schedulers.tornado import TornadoScheduler

# Parsing
import json
import csv
from io import StringIO

import os
import codecs
#import pprint

# profiling tools
from pympler import summary, muppy
import psutil


# Load template file templates/site.html
basedir='./'
TEMPLATE_FILE = 'prettyJSON.html'
templateLoader = FileSystemLoader( searchpath=basedir+"src/py/web-templates/" )
templateEnv = Environment( loader=templateLoader )
template = templateEnv.get_template(TEMPLATE_FILE)
static_path = basedir+'src/web/html/'
logdir = basedir+'log/'
appdir = basedir+'src/py/'
requestTemplatesDir = appdir+'http-req-templates/'
ahouse_path = basedir+'/src/web/'
netatmo_auth_file = basedir+'conf/netatmo_auth.json'
netatmo_token_file = basedir+'conf/netatmo_token.json'
netatmo_access_token = None
netatmo_access_expires = None

LOG_LEVEL = 20 # 50 critical, 40 error, 30 warning, 20 info, 10 debug
dump_json = False

bgloop_syncto = 60.0 # even seconds to sync to, called once when app starts
bgloop= 300.0 # default loop interval in seconds
dbAggregation_running = True # start on server startup
dbAggregation_runonce = False
dbCache_update = True # trigger cache refresh on bg loop

#dbPath = 'aHouseEnergy_2015mod.sqlite'
dbPath = basedir+'db/'
dbFile = dbPath+'aHouseEnergy.sqlite'
dbConn = None
dbCursor = None
_dbHelperMYSQL = None
_dbHelperSQLITE = None

#APScheduler
_scheduler = None

#cache update request stack
_cacheupdstack = []

# 2015-11-10T07:46:13.771123Z 
df = '%Y-%m-%dT%H:%M:%S.%fZ'  # datetime format used in db, always UTC time, microsecond accuracy
dt0 = datetime.utcfromtimestamp(0)  # epoch as naive datetime object without timezone
dt0_with_tz = datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)  # epoch as datetime object with timezone (utc)




# sql filenames: key + '.sql'
sql_statements = {
    'create_table_aClimate':'',
    'create_table_aPower':'',
    'create_table_aPowerStats':'',
    'select_aPowerStats_all':''
    }


# DB helper function 
def setupsql():
    _log.info('Setup db, loading sql-statements from files')
    for key in sql_statements:
        with open(dbPath+key+'.sql', 'r', encoding='utf-8') as sql_file:
            sql_statements[key] = sql_file.read()
        _log.debug('file : %s sql : \n%s' % (key,sql_statements[key]))
    return

@gen.coroutine
def parseRequestDates(args):
    # expecting date format %Y%m%d , 20150124 
    _log.debug('Darsing dates from request, args: %s' % args)
    mindate = args['mindate'] if args and 'mindate' in args else None 
    maxdate = args['maxdate'] if args and 'maxdate' in args else None 
    tzname = args['tzname'] if args and 'tzname' in args else 'UTC' 

    # create pd.Timestamp (in tz) from mindate(in tz) and maxdate(in tz)
    if mindate:
        mints = pd.to_datetime(mindate,format='%Y%m%d').tz_localize(tzname)
    else:
        # today(in tz) 00:00
        mints = pd.Timestamp(datetime.utcnow()).tz_localize('UTC').tz_convert(tzname) - pd.tseries.offsets.Day(normalize=True) + pd.tseries.offsets.Day() # today at 00:00 in tz
    if maxdate:
        maxts = pd.to_datetime(maxdate,format='%Y%m%d').tz_localize(tzname)+ pd.tseries.offsets.Day() - pd.tseries.offsets.Nano() # maxdate at 1us before midnight in tz
    else:
        # today 00:00
        maxts = pd.Timestamp(datetime.utcnow()).tz_localize('UTC').tz_convert(tzname) + pd.tseries.offsets.Day(normalize=True) - pd.tseries.offsets.Nano() # today at 1us  before midnight in tz
    _log.debug('Min ts = %s' % mints)
    _log.debug('Max ts = %s' % maxts)
    args['mints']=mints
    args['maxts']=maxts
    return args


def get_virtual_memory_usage_kb():
    """
    The process's current virtual memory size in Kb, as a float.

    """
    return float(psutil.Process().memory_info_ex().vms) / 1024.0

def memory_usage(where):
    """
    Print out a basic summary of memory usage.

    """
    mem_summary = summary.summarize(muppy.get_objects())
    print ("Memory summary: %s", where)
    summary.print_(mem_summary, limit=2)
    print ("VM: %.2fMb" % (get_virtual_memory_usage_kb() / 1024.0))

class DBHelperMYSQL(object):
    def __init__(self):
        self.dbexecutor = ThreadPoolExecutor(10)
        self.dbPool = None
        self.dbSAPool = None
        self.dbCursor = None
        self.dataCaches = None
        self.base = None
        self.devToCacheMap = None

    @gen.coroutine
    def haveCaches(self,args):
        cmap = self.devToCacheMap  
        cacheList = {}  # dict of 'datatype':'cachename' - pairs
        if cmap:
            _log.info('have map')
            if args['device'] and args['device'] in cmap:
                _log.info('have device %s in map' % args['device'])
                for dt in args['datatypes']:
                    _log.info ('checking for %s' % dt)
                    if dt in cmap[args['device']]:
                        _log.info('have  %s = %s in map' % (dt,cmap[args['device']][dt]))
                        cacheList[dt] = cmap[args['device']][dt]
                    else:
                        _log.info('missing %s ' % dt)
                        cacheList = None
                        return cacheList # fail fast at first missing value
            else:
                cacheList = None
                return cacheList # fail fast at first missing value
        else:
            cacheList = None
            return cacheList # fail fast at first missing value
        _log.info('required cachelist is %s' % cacheList)
        # check that all caches available, fail fast on first missing
        caches = self.dataCaches
        for k,v in cacheList.items():
            _log.info('looking for %s in caches' % v)
            if v not in caches or caches[v] is None or caches[v]['cache'] is None:
                _log.info('%s not available' % v)
                cacheList = None
        _log.info('required and available cachelist is %s' % cacheList)
        return cacheList

    @gen.coroutine
    def prepareDataCache(self):
        _log.info('Preparing to initialize data caches')
        self.dataCaches = {
                'outdoortemperature': {
                    'columns': ['value'],
                    'sql':'SELECT eventNanoTs,value FROM aClimateData WHERE sourceId="02-00-00-03-08-1c" AND eventType="Temperature"',
                    'index_col':['eventNanoTs'],
                    'parse_dates':{'eventNanoTs':'ns'},
                    # 'parse_dates':{'eventNanoTs':'%Y-%m-%dT%H:%M:%S.%fZ'},
                    'cache':None
                # },
                # 'outdoorrest': {
                #     'columns': ['value'],
                #     'sql':'SELECT eventNanoTs,eventType,value FROM aClimateData WHERE sourceId="02-00-00-03-08-1c" AND eventType != "Temperature"',
                #     'index_col':['eventNanoTs','eventType'],
                #     'parse_dates':{'eventNanoTs':'ns'},
                #     'cache':None
                # },
                # 'indoortemperature': {
                #     'columns': ['value'],
                #     'sql':'SELECT eventNanoTs,sourceId,value FROM aClimateData WHERE eventType="Temperature" AND (sourceId="70-ee-50-02-d4-2c" OR sourceId="03-00-00-01-21-a2")',
                #     'index_col':['eventNanoTs','sourceId'],
                #     'parse_dates':{'eventNanoTs':'ns'},
                #     'cache':None
                # },
                # 'indoor1rest': {
                #     'columns': ['value'],
                #     'sql':'SELECT eventNanoTs,eventType,value FROM aClimateData WHERE sourceId="70-ee-50-02-d4-2c" AND eventType != "Temperature"',
                #     'index_col':['eventNanoTs','eventType'],
                #     'parse_dates':{'eventNanoTs':'ns'},
                #     'cache':None
                # },
                # 'indoor2rest': {
                #     'columns': ['value'],
                #     'sql':'SELECT eventNanoTs,eventType,value FROM aClimateData WHERE sourceId="03-00-00-01-21-a2" AND eventType != "Temperature"',
                #     'index_col':['eventNanoTs','eventType'],
                #     'parse_dates':{'eventNanoTs':'ns'},
                #     'cache':None
                # },
                # 'outdoorrain': {
                #     'columns': ['value'],
                #     'sql':'SELECT eventNanoTs,value FROM aClimateData WHERE sourceId="05-00-00-00-16-88" AND eventType="Rain"',
                #     'index_col':['eventNanoTs'],
                #     'parse_dates':{'eventNanoTs':'ns'},
                #     'cache':None
                },
                'energywater': {
                    'columns': ['c1_delta','c1_cumul','c1_peak','c1_indirect','c2_delta','c2_cumul','c2_peak','c2_indirect','c3_delta','c3_cumul','c3_peak','c3_indirect','measures_count'],
                    'sql':'SELECT groupNanoTs as eventNanoTs,c1_delta,c1_cumul,c1_peak,c1_indirect,c2_delta,c2_cumul,c2_peak,c3_indirect,c3_delta,c3_cumul,c3_peak,c3_indirect,measures_count FROM aWaterEnergyStats',
                    'index_col':['eventNanoTs'],
                    'parse_dates':{'eventNanoTs':'ns'},
                    'cache':None
                # },
                # 'energyfloor': {
                #     'columns': ['accessNumber','heatingEnergy','volume','temp1','temp2','power','flow'],
                #     'sql':'SELECT recordNanoTs,accessNumber,heatingEnergy,volume,temp1,temp2,power,flow FROM aMbusMC302Record WHERE id="67285016"',
                #     'index_col':['recordNanoTs'],
                #     'parse_dates':{'recordNanoTs':'ns'},
                #     'cache':None
                # },
                # 'energywoodboiler': {
                #     'columns': ['accessNumber','heatingEnergy','volume','temp1','temp2','power','flow'],
                #     'sql':'SELECT recordNanoTs,accessNumber,heatingEnergy,volume,temp1,temp2,power,flow FROM aMbusMC302Record WHERE id="67285015"',
                #     'index_col':['recordNanoTs'],
                #     'parse_dates':{'recordNanoTs':'ns'},
                #     'cache':None
                # },
                # 'energywarmwater': {
                #     'columns': ['accessNumber','heatingEnergy','volume','temp1','temp2','power','flow'],
                #     'sql':'SELECT recordNanoTs,accessNumber,heatingEnergy,volume,temp1,temp2,power,flow FROM aMbusMC302Record WHERE id="67280331"',
                #     'index_col':['recordNanoTs'],
                #     'parse_dates':{'recordNanoTs':'ns'},
                #     'cache':None
                },
                'energyelectricity': {
                    'columns': ['c1_use','c1_cumul','c1_peak','c2_use','c2_cumul','c2_peak','c3_use','c3_cumul','c3_peak','c4_use','c4_cumul','c4_peak','c5_use','c5_cumul','c5_peak','c6_use','c6_cumul','c6_peak','c7_use','c7_cumul','c7_peak','c8_use','c8_cumul','c8_peak','all_use','all_cumul','all_peak','measures_count'],
                    'sql':'SELECT groupNanoTs as eventNanoTs,c1_use,c1_cumul,c1_peak,c2_use,c2_cumul,c2_peak,c3_use,c3_cumul,c3_peak,c4_use,c4_cumul,c4_peak,c5_use,c5_cumul,c5_peak,c6_use,c6_cumul,c6_peak,c7_use,c7_cumul,c7_peak,c8_use,c8_cumul,c8_peak,all_use,all_cumul,all_peak,measures_count FROM aPowerStats',
                    'index_col':['eventNanoTs'],
                    'parse_dates':{'eventNanoTs':'ns'},
                    'cache':None
                # },
                # 'wholetable': {
                #     'columns': ['value'],
                #     'sql':'SELECT eventNanoTs,sourceId,eventType,value FROM aClimateData',
                #     'index_col':['eventNanoTs','sourceId','eventType'],
                #     'parse_dates':{'eventNanoTs':'ns'},
                #     'cache':None
                }
            }
        self.devToCacheMap = {
                '02-00-00-03-08-1c': {
                    'Temperature':'outdoortemperature',
                    'Humidity':'outdoorrest',
                    },
                '70-ee-50-02-d4-2c': {
                    'Temperature':'indoortemperature',
                    'Humidity':'indoor1rest',
                    'Pressure':'indoor1rest',
                    'CO2':'indoor1rest',
                    'Noise':'indoor1rest'
                    },
                '03-00-00-01-21-a2': {
                    'Temperature':'indoortemperature',
                    'Humidity':'indoor1rest',
                    'Co2':'indoor1rest'
                },
                '05-00-00-00-16-88': {
                    'Temperature':'indoortemperature',
                    'Humidity':'indoor1rest',
                    'CO2':'indoor1rest'
                },
                '67285016':{
                    'accessNumber':'energyfloor',
                    'heatingEnergy':'energyfloor',
                    'volume':'energyfloor',
                    'temp1':'energyfloor',
                    'temp2':'energyfloor',
                    'power':'energyfloor',
                    'flow':'energyfloor'
                },
                '67285015':{
                    'accessNumber':'energywoodboiler',
                    'heatingEnergy':'energywoodboiler',
                    'volume':'energywoodboiler',
                    'temp1':'energywoodboiler',
                    'temp2':'energywoodboiler',
                    'power':'energywoodboiler',
                    'flow':'energywoodboiler'
                },
                '67280331':{
                    'accessNumber':'energywarmwater',
                    'heatingEnergy':'energywarmwater',
                    'volume':'energywarmwater',
                    'temp1':'energywarmwater',
                    'temp2':'energywarmwater',
                    'power':'energywarmwater',
                    'flow':'energywarmwater'
                },
                'water':{
                    'c1_delta':'energywater',
                    'c1_cumul':'energywater',
                    'c1_peak':'energywater',
                    'c2_delta':'energywater',
                    'c2_cumul':'energywater',
                    'c2_peak':'energywater',
                    'c3_delta':'energywater',
                    'c3_cumul':'energywater',
                    'c3_peak':'energywater',
                },
                'electricity':{
                    'c1_use':'energyelectricity',
                    'c1_cumul':'energyelectricity',
                    'c1_peak':'energyelectricity',
                    'c2_use':'energyelectricity',
                    'c2_cumul':'energyelectricity',
                    'c2_peak':'energyelectricity',
                    'c3_use':'energyelectricity',
                    'c3_cumul':'energyelectricity',
                    'c3_peak':'energyelectricity',
                    'c4_use':'energyelectricity',
                    'c4_cumul':'energyelectricity',
                    'c4_peak':'energyelectricity',
                    'c5_use':'energyelectricity',
                    'c5_cumul':'energyelectricity',
                    'c5_peak':'energyelectricity',
                    'c6_use':'energyelectricity',
                    'c6_cumul':'energyelectricity',
                    'c6_peak':'energyelectricity',
                    'c7_use':'energyelectricity',
                    'c7_cumul':'energyelectricity',
                    'c7_peak':'energyelectricity',
                    'c8_use':'energyelectricity',
                    'c8_cumul':'energyelectricity',
                    'c8_peak':'energyelectricity',
                    'all_use':'energyelectricity',
                    'all_cumul':'energyelectricity',
                    'all_peak':'energyelectricity',
                    'measures_count':'energyelectricity'
                }

            }    
        return self.dataCaches

    @gen.coroutine
    def refreshDataCache(self):
        global _cacheupdstack
        profile_mem = False
        if profile_mem:
            _log.info('Calculating mem usage')
            memory_usage('')
        
        if not len(_cacheupdstack):
            _log.info('No cache updates in queue')
        else:
            _log.info('Preparing to initialize caches : %s' % _cacheupdstack)

            if self.dataCaches is None:
                res = yield self.prepareDataCache()

            starttime = time.time()

            # ({'device':'02-00-00-03-08-1c','datatype':'Temperature,Humidity','loctype':'outdoor','sourceType':'netatmo'})
            # ({'device':'70-ee-50-02-d4-2c','datatype':'Temperature,Humidity,CO2,Pressure,Noise','loctype':'indoor','sourceType':'netatmo'})
            # ({'device':'03-00-00-01-21-a2','datatype':'Temperature,Humidity,CO2','loctype':'indoor','sourceType':'netatmo'})
            # ({'device':'05-00-00-00-16-88','datatype':'Rain','loctype':'outdoor','sourceType':'netatmo'})

            while len(_cacheupdstack):
                c = _cacheupdstack.pop(0)
                if c == 'all':
                    _cacheupdstack = [] # dump rest of stack since 'all' caches are updated
                    for name,val in self.dataCaches.items():
                        res = yield DBHelperMYSQL.initDataCache(self,params=val,name=name)
                        val['cache'] = res
                else:
                    if c in self.dataCaches:
                        _log.info("Updating cache '%s'" % c)
                        res = yield DBHelperMYSQL.initDataCache(self,params=self.dataCaches[c],name=c)
                        self.dataCaches[c]['cache'] = res
                    else:
                        _log.warning('Invalid cache refresh requested : %s' % c)


            _log.info('All caches ready in %.3fs' % (time.time()-starttime))
        if profile_mem:
            _log.info('Calculating mem usage')
            memory_usage('')
        return

    @run_on_executor(executor='dbexecutor')
    def initDataCache(self,params=None,name=''):
        start = time.time()
        #conn = pymysql.connect(unix_socket='/var/run/mysqld/mysqld.sock',host='localhost', user='aHouseDbUser', passwd='', db='aHouseEnergy')  # connect synchronously on threadpool using pymysql because pandas doesn't yield
        #engine = create_engine('mysql+pymysql://aHouseDbUser:@localhost:3306/aHouseEnergy', echo=False)
        #engine = create_engine('mysql+pymysql://aHouseDbUser:@localhost:3306/aHouseEnergy', echo=False, connect_args={'cursorclass': pymysql.cursors.SSCursor})

        #engine = create_engine('mysql+mysqldb://aHouseDbUser:@localhost:3306/aHouseEnergy', echo=False)
        chunking = True
        chnksiz = 10000

        engine = create_engine('mysql+mysqldb://aHouseDbUser:@localhost:3306/aHouseEnergy', echo=False, connect_args={'cursorclass': MySQLdb.cursors.SSCursor})
        conn = engine.connect().execution_options(stream_results=True)

        _log.info('Init data cache \'%s\' with params:' % name)
        _log.info('sql: %s' % params['sql'])
        _log.info('index_col: %s' % params['index_col'])
        _log.info('parse_dates: %s' % params['parse_dates'])

        if not chunking:
            cache = pd.read_sql_query(params['sql'],conn,index_col=params['index_col'],parse_dates=params['parse_dates'])
            cache.columns=params['columns']
        else:
            listofchunks = []
            for chunk in pd.read_sql_query(params['sql'],conn,index_col=params['index_col'],parse_dates=params['parse_dates'],chunksize=chnksiz):
                listofchunks.append(chunk)
            cache = pd.concat(listofchunks)
            cache.columns=params['columns']
        conn.close()
        info = StringIO()
        cache.info(buf=info)
        _log.info('Cache \'%s\' ready in %.3fs, info : \n%s' % (name,(time.time()-start),info.getvalue()))
        return cache

    @run_on_executor(executor='dbexecutor')
    def dbConnect(self):

        pools.DEBUG = True

        self.dbPool = pools.Pool(
            dict(host='127.0.0.1', port=3306, user='aHouseDbUser', passwd='', db='aHouseEnergy'),
            max_idle_connections=10,
            max_recycle_sec=3)

    @run_on_executor(executor='dbexecutor')
    def dbInsertDataFrame(self,df,tablename):
        start = time.time()

        # engine = create_engine('mysql+mysqldb://aHouseDbUser:@localhost:3306/aHouseEnergy', echo=False, connect_args={'cursorclass': MySQLdb.cursors.SSCursor})
        # conn = engine.connect().execution_options(stream_results=True)
        engine = create_engine('mysql+mysqldb://aHouseDbUser:@localhost:3306/aHouseEnergy', echo=False)

        try:
            result = df.to_sql(tablename,engine,if_exists='append')
        except Exception as e:
            _log.warning('Error during database operation : %s' % str(e))
            pass
        #conn.close()
        _log.info('Db insert ready in %.3fs , result : %s' % (time.time()-start,result))
        return 

    @gen.coroutine
    def dbExecute(self,query,parseresp=None,returnrowcount=False):
        if not self.dbPool:
            _log.info('Db connection not ready, connecting to db')
            res = yield self.dbConnect()
            _log.info('Connected to db')

        res = yield DBHelperMYSQL._dbExecute(self,query,parseresp,returnrowcount)
        return res

    @gen.coroutine
    def _dbExecute(self,query,parseresp,returnrowcount):
        _log.debug('Executing SQL')
        try:
            self.dbCursor = yield self.dbPool.execute(query)

            result = None
            if parseresp is not None:
                if parseresp == 'CSV':
                    tmp = StringIO()
                    csv_writer = csv.writer(tmp, quoting=csv.QUOTE_NONNUMERIC)
                    csv_writer.writerow([i[0] for i in self.dbCursor.description]) # write headers
                    csv_writer.writerows(self.dbCursor)
                    result = tmp.getvalue()
                    _log.debug('Db result CSV :\n%s' % result)
            else:
                if returnrowcount: 
                    result = self.dbCursor.rowcount
                    if result:
                        _log.debug('Db rowcount :\n%d' % result)
                else:
                    result = self.dbCursor.fetchall()
                    if result:
                        _log.debug('DB result :\n%s' % result)


        except Exception:
            raise
        return result

class DBAggregator(object):
    @gen.coroutine
    def aggregateMC302():

        url = 'http://abox.local:8888/data?q=mbus'
        _log.info('Get timestamp of latest update...')

        sql = 'select recordTimestamp from aMbusMC302Record order by recordNanoTs DESC LIMIT 1;\n'
        _log.debug('SQL: \n%s' % sql)
        result = None
        try:
            result = yield DBHelperMYSQL.dbExecute(_dbHelperMYSQL,sql,parseresp=None)
        except Exception as e:
            _log.warning('Error during database operation : %s' % str(e))
            pass

        urlsuffix = None if result[0] is None else '&mints=%s' % result[0]

        url += urlsuffix

        _log.info('Getting data url: %s' % url)

        http_request = HTTPRequest(url)   
        http_client = AsyncHTTPClient()
        try:
            httpresponse = yield http_client.fetch(http_request)

            d = pd.read_csv(httpresponse.buffer)
            _log.info('HTTP Response : %s - %s, Got %d new rows' % (httpresponse.code,httpresponse.reason,len(d.index)))

            # add 'recordNanoTs' and drop rowId 
            d['recordNanoTs'] = pd.to_datetime(d['recordTimestamp'],format=df).astype(np.int64)
            d.set_index('recordNanoTs',inplace=True)
            d.drop('rowId',inplace=True,axis=1)

            # write  DataFrame to DB
            result = yield DBHelperMYSQL.dbInsertDataFrame(_dbHelperMYSQL,d,'aMbusMC302Record')

            # aggregate waterEnergyStats
            # find latest 'groupNanoTs' in aWaterEnergyStats        
            sql = 'select groupNanoTs from aWaterEnergyStats order by groupNanoTs DESC LIMIT 1;\n'
            _log.debug('SQL: \n%s' % sql)
            result = None
            result = yield DBHelperMYSQL.dbExecute(_dbHelperMYSQL,sql,parseresp=None)
            gts = 0 if result[0] is None else '%s' % result[0]

            with open(dbPath+'aggregateWaterEnergyStats.sql','r',encoding='utf-8') as sql_file:
                sql = sql_file.read() 
            
            sql = sql.replace(';REPLACEWITHNANOSEC;',gts)
            _log.debug('SQL: \n%s' % sql)
            result = yield DBHelperMYSQL.dbExecute(_dbHelperMYSQL,sql,parseresp=None,returnrowcount=True)

            _log.info('Aggregation of aWaterEnergyStats updated %s rows' % result)

            result > 0 and _cacheupdstack.append('energyelectricity')
            
        except HTTPError as e:
            _log.warning('HTTPError \'%s : %s\' when requesting MC302 data, skipping...' %  (e.code,e.message))
            pass
        except Exception as e:
            # Other errors are possible, such as IOError.
            _log.error("Error: " + str(e))
            pass     
        return

    @gen.coroutine
    def aggregatePower():

        url = 'http://abox.local:8888/data?q=power'
        _log.info('Get timestamp of latest update...')

        sql = 'select ts from aPower order by eventNanoTs DESC LIMIT 1;\n'
        _log.debug('SQL: \n%s' % sql)
        result = None
        try:
            result = yield DBHelperMYSQL.dbExecute(_dbHelperMYSQL,sql,parseresp=None)
        except Exception as e:
            _log.warning('Error during database operation : %s' % str(e))
            pass

        urlsuffix = None if result[0] is None else '&mints=%s' % tornado.escape.url_escape('%s' % result[0])
        
        url += urlsuffix

        _log.info('Getting data url: %s' % url)

        http_request = HTTPRequest(url)   
        http_client = AsyncHTTPClient()
        try:
            httpresponse = yield http_client.fetch(http_request)

            d = pd.read_csv(httpresponse.buffer)
            _log.info('HTTP Response : %s - %s, Got %d new rows' % (httpresponse.code,httpresponse.reason,len(d.index)))
            dateformat = '%Y-%m-%d %H:%M:%S.%f'
            # add 'recordNanoTs' and drop rowId 
            d['eventNanoTs'] = pd.to_datetime(d['ts'],format=dateformat).astype(np.int64)
            d.set_index('eventNanoTs',inplace=True)
            d.drop('id',inplace=True,axis=1)

            # write  DataFrame to DB
            result = yield DBHelperMYSQL.dbInsertDataFrame(_dbHelperMYSQL,d,'aPower')

            # aggregate aPowerStats
            # find latest 'groupNanoTs' in aPowerStats        
            sql = 'select groupNanoTs from aPowerStats order by groupNanoTs DESC LIMIT 1;\n'
            _log.debug('SQL: \n%s' % sql)
            result = None
            result = yield DBHelperMYSQL.dbExecute(_dbHelperMYSQL,sql,parseresp=None)
            gts = 0 if result[0] is None else '%s' % result[0]

            with open(dbPath+'aggregatePowerStats.sql','r',encoding='utf-8') as sql_file:
                sql = sql_file.read() 
            
            sql = sql.replace(';REPLACEWITHNANOSEC;',gts)
            _log.debug('SQL: \n%s' % sql)
            result = yield DBHelperMYSQL.dbExecute(_dbHelperMYSQL,sql,parseresp=None,returnrowcount=True)

            _log.info('Aggregation of aPowerStats updated %s rows' % result)

            result > 0 and _cacheupdstack.append('energyelectricity')
            
        except HTTPError as e:
            _log.warning('HTTPError \'%s : %s\' when requesting Power data, skipping...' %  (e.code,e.message))
            pass
        except Exception as e:
            # Other errors are possible, such as IOError.
            _log.error("Error: " + str(e))
            pass     
        return


class TaskRunner(object):
    def __init__(self, loop):
        self.executor = ThreadPoolExecutor(4)
        self.arguments = {}

    def get_argument(self, name, default, strip=True):
        return self.arguments[name] if name in  self.arguments else default

    def set_argument(self, name, value):
        self.arguments[name] = value
        return 

    @gen.coroutine
    def bgheartbeat():
        out = StringIO() 
        _scheduler.print_jobs(out=out)
        _log.info('%s' % out.getvalue())

    @gen.coroutine
    def setupbg(self):
        
        _cacheupdstack.append('all')
        offset = 30.0 # offset x seconds
        firstrun = datetime.fromtimestamp(divmod(datetime.now().timestamp(),bgloop_syncto)[0]*bgloop_syncto+bgloop_syncto+offset)
        _log.info('Background jobs first run at %s' % firstrun)

        _scheduler.add_job(TaskRunner.bgheartbeat, 'interval', seconds=60,next_run_time=firstrun)
        _scheduler.add_job(DBAggregator.aggregateMC302, 'interval', seconds=60,next_run_time=firstrun)
        _scheduler.add_job(DBAggregator.aggregatePower, 'interval', seconds=60,next_run_time=firstrun)
        _scheduler.add_job(NetatmoHelper.retrieveAndStoreAllTypesNetatmoData, 'interval',args=[self],seconds=300,next_run_time=firstrun)
        _scheduler.add_job(DBHelperMYSQL.refreshDataCache, 'interval',args=[_dbHelperMYSQL],seconds=15)   

        out = StringIO() 
        _scheduler.print_jobs(out=out)
        _log.info('%s' % out.getvalue())
        return

class NetatmoHelper(object):
    @gen.coroutine
    def dbInsertClimateEvents(self,data,args):
        _log.debug('Starting db insert...')

        sqlbuf = StringIO()
        _log.debug(data)
        for k,v in sorted(data.items()):
            #print ('k : %s , v[0] : %s' % (k,v[0]))
            datatypes = args['datatype'].split(',')
            for i, val in enumerate(datatypes):
                _log.debug('Ts: %s , Datatype: %s, Value: %s' % (k,val,v[i]))
                if v[i] is not None:
                    #sqlite
                    # sqlbuf.write('INSERT INTO aClimateData (sourceType,sourceId,eventType,value,locationType,eventTimestamp,rowTimestamp) \
                    #     VALUES (\'netatmo\',\'%s\',\'%s\',%s,\'%s\',\'%s\',\'%s\');\n' % \
                    #     (args['device'],val,v[i],args['loctype'],datetime.utcfromtimestamp(float(k)).strftime(df),datetime.utcnow().strftime(df)))
                    #mysql
                    sqlbuf.write('INSERT INTO aClimateData (eventNanoTs,sourceType,sourceId,eventType,value,locationType,eventTimestamp,rowTimestamp) \
                        VALUES (%d,\'netatmo\',\'%s\',\'%s\',%s,\'%s\',\'%s\',\'%s\');\n' % \
                        ((float(k)*10**9),args['device'],val,v[i],args['loctype'],datetime.utcfromtimestamp(float(k)).strftime(df),datetime.utcnow().strftime(df)))

        _log.debug('SQL: \n%s' % sqlbuf.getvalue())
        try:
            #res = yield DBHelperSQLITE.dbExecute(_dbHelperSQLITE,sqlbuf.getvalue(),parseresp=None,tzname=None,script=True)
            res = yield DBHelperMYSQL.dbExecute(_dbHelperMYSQL,sqlbuf.getvalue(),parseresp=None,returnrowcount=True)
            res > 0 and _cacheupdstack.append('all')
        except Exception as e:
            _log.warning('Error during database inserts : %s' % str(e))
            raise
        _log.debug('Finishing db insert...')
        return

    @gen.coroutine
    def dbGetClimateEventLastUpdate(self,args):
        _log.debug('Get timestamp of latest update...')

        # Get timestamp of oldest entry of the most recent entries of each type (MYSQL) 
        # SELECT MIN(tmp.ts) AS minofmaxes FROM 
        # (   
        #   (SELECT eventNanoTs AS ts FROM `aClimateData` WHERE sourceId='02-00-00-03-08-1c' AND eventType='Temperature' ORDER BY rowid DESC LIMIT 1)
        #     UNION
        #   (SELECT eventNanoTs AS ts FROM `aClimateData` WHERE sourceId='02-00-00-03-08-1c' AND eventType='Humidity' ORDER BY rowid DESC LIMIT 1)
        # )  AS tmp

        sqlbuf = StringIO()

        sqlbuf.write('SELECT MIN(tmp.ts) AS minofmaxes FROM\n(\n')
        datatypes = args['datatype'].split(',')
        for i,val in enumerate(datatypes):
            if (i != 0): sqlbuf.write('UNION\n')
            sqlbuf.write('  (SELECT eventNanoTs AS ts FROM aClimateData WHERE sourceId= \'%s\' AND eventType=\'%s\' ORDER BY rowid DESC LIMIT 1)\n' % (args['device'],val))
        sqlbuf.write(') AS tmp\n')

        _log.debug('SQL: \n%s' % sqlbuf.getvalue())
        result = None
        try:
            result = yield DBHelperMYSQL.dbExecute(_dbHelperMYSQL,sqlbuf.getvalue(),parseresp=None)
        except Exception as e:
            _log.warning('Error during database read : %s' % str(e))
            pass
        _log.debug('result: %s' % result)
        _log.debug('result[0]: %s' % result[0])
        _log.debug('type(result[0]): %s' % type(result[0])) # mysql returns tuple!
        #mysql (nanosec)
        ts = float(0) if result[0] is None else (float('%s' % result[0])/10**9)  # in nanosecs if mysql
        _log.info('Got timestamp \'%s\' : %f' % (result[0],ts))
        return ts

    @gen.coroutine
    def retrieveAndStoreAllTypesNetatmoData(self):

        token = yield NetatmoHelper.authenticateNetatmo(self)

        argstack = []
        # 70-ee-50-02-d4-2c   Inne    netatmoreq_template_70-ee-50-02-d4-2c.json
        # 02-00-00-03-08-1c   Ute     netatmoreq_template_02-00-00-03-08-1c.json
        # 03-00-00-01-21-a2   Bygge   netatmoreq_template_03-00-00-01-21-a2.json
        # 05-00-00-00-16-88   Regn    netatmoreq_template_05-00-00-00-16-88.json
        argstack.append({'device':'02-00-00-03-08-1c','datatype':'Temperature,Humidity','loctype':'outdoor','sourceType':'netatmo'})
        argstack.append({'device':'70-ee-50-02-d4-2c','datatype':'Temperature,Humidity,CO2,Pressure,Noise','loctype':'indoor','sourceType':'netatmo'})
        argstack.append({'device':'03-00-00-01-21-a2','datatype':'Temperature,Humidity,CO2','loctype':'indoor','sourceType':'netatmo'})
        argstack.append({'device':'05-00-00-00-16-88','datatype':'Rain','loctype':'outdoor','sourceType':'netatmo'})

        for args in argstack:
            _log.info('Processing args : %s' % args)
            self.set_argument('device',args['device'])
            self.set_argument('datatype',args['datatype'])
            self.set_argument('loctype',args['loctype'])
            self.set_argument('sourceType',args['sourceType'])
            res = yield NetatmoHelper.retrieveAndStoreNetatmoData(self,token)

    @gen.coroutine
    def retrieveAndStoreNetatmoData(self,token):
        
        min_s = "2014-04-10T00:00:00.000000Z"  # no data available previous to this
        now = time.time() # current time in epoch seconds (float)

        args = {}
        args['device']=self.arguments['device'] if self.arguments['device'] is not None else '02-00-00-03-08-1c'
        args['datatype']=self.arguments['datatype'] if self.arguments['datatype'] is not None else 'Temperature,Humidity'
        args['loctype']=self.arguments['loctype'] if self.arguments['loctype'] is not None else 'outdoor'
        args['sourceType']=self.arguments['sourceType'] if self.arguments['sourceType'] is not None else 'netatmo'

        _log.debug('Processing args : %s' % args)
        mints = yield NetatmoHelper.dbGetClimateEventLastUpdate(self,args)

        t1 = int(mints)+1 if mints > 0 else (datetime.strptime(min_s, df) - dt0).total_seconds()
        args['begin']=self.arguments['begin']=t1

        args['end']=self.arguments['end']=int(now)     # get all data up until now (netatmo limits to 1024 entries per response)

        jsondata = yield NetatmoHelper.getNetatmoMeasureData(self,token)
        ##   {"body":{"1397154014":[12.7,68],"1397154276":[15.3,35]
        data = tornado.escape.json_decode(jsondata)
        _log.debug('Response data : %s' % jsondata)
        if 'body' in data and len(data['body']) > 0 :
            l = len(data['body'])
            _log.info('Got data : %d entries : %s - %s' % (l,datetime.fromtimestamp(float(list(data['body'].keys())[0])).strftime(df),datetime.fromtimestamp(float(list(data['body'].keys())[l-1])).strftime(df)))
            _log.debug('Calling DB update...')
            res = yield NetatmoHelper.dbInsertClimateEvents(self,data['body'],args)
            _log.debug('After data update...')
        else:
            _log.info('No new data received')
        return

    @gen.coroutine
    def getNetatmoDashboardData(self,token):
        print ('Getting dashboard data.....')
        u = 'https://api.netatmo.com/api/getstationsdata?access_token='+token
        http_request = HTTPRequest(u)   
        http_client = AsyncHTTPClient()
        try:
            httpresponse = yield http_client.fetch(http_request)
            responseJson = tornado.escape.to_basestring(httpresponse.body)
            responseDict = json.loads(tornado.escape.to_basestring(httpresponse.body))

            #print ('Dash response JSON: %s' % responseJson)
            epoch_time = int(time.time())
            if dump_json:
                with open((logdir+'netatmo_station_dump_%s.json' % epoch_time), 'w', encoding='utf-8') as resp_file:
                    resp_file.write(tornado.escape.json_encode(responseDict))
                with open((logdir+'netatmo_station_dump_%s.json.meta' % epoch_time), 'w', encoding='utf-8') as meta_file: 
                    meta_file.write(str(u))
            # with open('netatmo_getstationdata_example.json', 'w', encoding='utf-8') as data_file:
            #     data_file.write(responseJson)
            _log.info('HTTP Response : %s - %s' % (httpresponse.code,httpresponse.reason))
            return responseJson
        except HTTPError as e:
            _log.warning('HTTPError \'%s : %s\' when requesting Netatmo authentication token, skipping...' %  (e.code,e.message))
            pass
        except Exception as e:
            # Other errors are possible, such as IOError.
            _log.error("Error: " + str(e))
            raise     

        return None

    @gen.coroutine
    def authenticateNetatmo(self):
        global netatmo_access_token
        global netatmo_access_expires

        token_minimi_life = 600     # minimi delta in seconds

        if (netatmo_access_token is not None and netatmo_access_expires is not None and netatmo_access_expires-time.time() < token_minimi_life):
            return netatmo_access_token
        else:
            _log.info('Doing authentication to Netatmo ')

            refresh_token = None
            if os.path.isfile(netatmo_token_file):
                token = None
                with open(netatmo_token_file, 'r', encoding='utf-8') as token_file:
                    token = json.loads(token_file.read())
                if token is not None:
                    delta = token['created']+token['expires_in']-time.time()
                    _log.info('Token valid for %d seconds still' % delta )
                    if delta > token_minimi_life:
                        netatmo_access_token = token['access_token']
                        netatmo_access_expires = token['created'] + token['expires_in']
                        return netatmo_access_token
                    elif delta > 60:
                        # trying refresh if more than 1minute left before token expiration
                        _log.info('Trying to refresh token')
                        refresh_token = token['refresh_token'] 

            data = None
            with open(netatmo_auth_file,'r', encoding='utf-8') as data_file:
                data = json.loads(data_file.read())             
            if data is not None:
                # z = {**x, **y} , merge two dictionaries, new in Python 3.5-> 
                if refresh_token is None:
                    b = data['common_body'].copy()
                    b.update(data['auth_body'])
                    #b = {**data['common_body'], **data['auth_body']}
                else:
                    b = data['refresh_body'].copy()
                    b.update(data['common_body'])
                    #b = {**data['refresh_body'], **data['common_body']}
                    b['refresh_token']=refresh_token
                    _log.info('Requesting refresh with refresh_token : %s'% b['refresh_token'])
                body = '&'.join(['%s=%s' % (key, value) for (key, value) in b.items()])
                _log.debug('HTTP Request Body \'%s\'' % body)
                http_request = HTTPRequest(
                    url=data['auth_url'],   # same for both auth and refresh 
                    method='POST',
                    follow_redirects=True,
                    headers=data['headers'],
                    body=body
                    )
                http_client = AsyncHTTPClient()
                try:
                    httpresponse = yield http_client.fetch(http_request)
                    responseDict = json.loads(tornado.escape.to_basestring(httpresponse.body))
                    
                    _log.info('New access token : %s' % responseDict['access_token'])
                    _log.info('Expires in : %d' % responseDict['expires_in'])
                    # Add creation time (in seconds since Epoch)
                    responseDict['created']=time.time()
                    
                    netatmo_access_token=responseDict['access_token']
                    netatmo_access_expires=responseDict['created']+responseDict['expires_in']

                    with open(netatmo_token_file, 'w', encoding='utf-8') as token_file:
                        token_file.write(tornado.escape.json_encode(responseDict))
                    
                except HTTPError as e:
                    _log.warning('HTTPError \'%s : %s\' when requesting Netatmo authentication token, skipping...' %  (e.code,e.message))
                    pass
                except Exception as e:
                    # Other errors are possible, such as IOError.
                    _log.error("Error: " + str(e))       
            return netatmo_access_token

    @gen.coroutine
    def getNetatmoMeasureData(self,token):

        _log.debug('Getting measurements data.....')
        # 70-ee-50-02-d4-2c   Inne    netatmoreq_template_70-ee-50-02-d4-2c.json
        # 02-00-00-03-08-1c   Ute     netatmoreq_template_02-00-00-03-08-1c.json
        # 03-00-00-01-21-a2   Bygge   netatmoreq_template_03-00-00-01-21-a2.json
        # 05-00-00-00-16-88   Regn    netatmoreq_template_05-00-00-00-16-88.json

        # Netatmo (https://dev.netatmo.com/doc/methods/getmeasure)
        # Please note, the measurements sent in the response will be in the same order as in this list. 
        # All measurements are expressed in metric units:
        # Temperature: Celsius
        # Humidity: %
        # Co2: ppm
        # Pressure: mbar
        # Noise: db
        # Rain: mm
        # Wind: speed in km/h and direction in °

        device = self.get_argument('device','70-ee-50-02-d4-2c')

        with open((requestTemplatesDir+'netatmoreq_template_%s.json' % device),'r', encoding='utf-8') as req_file:
            _log.debug('Found template file')
            req = json.loads(req_file.read())
            _log.debug('Loaded dictionary')

            if req is not None:
                epoch_time = int(time.time())

                req['body']['access_token'] = netatmo_access_token
                req['body']['date_begin'] = self.get_argument('begin', epoch_time-3600)
                req['body']['date_end'] = self.get_argument('end', epoch_time)
                req['body']['scale'] = self.get_argument('scale', req['body']['scale'])
                req['body']['type'] = self.get_argument('datatype', req['body']['type'])

                _log.debug(req)
                body = '&'.join(['%s=%s' % (k, v) for (k, v) in req['body'].items()])
                _log.debug('HTTP Request Body \'%s\'' % body)
                http_request = HTTPRequest(
                    url=req['url'], # same for both auth and refresh 
                    method='POST',
                    headers=req['headers'],
                    body=body
                    )
                http_client = AsyncHTTPClient()
                try:
                    httpresponse = yield http_client.fetch(http_request)
                    responseJson = tornado.escape.to_basestring(httpresponse.body)
                    responseDict = tornado.escape.json_decode(httpresponse.body)
                    if dump_json:
                        with open((logdir+'netatmo_measure_dump_%s.json' % epoch_time), 'w', encoding='utf-8') as resp_file:
                            resp_file.write(tornado.escape.json_encode(responseDict))
                        with open((logdir+'netatmo_measure_dump_%s.json.meta' % epoch_time), 'w', encoding='utf-8') as meta_file: 
                            meta_file.write(body)
                    _log.debug('HTTP Response : %s - %s' % (httpresponse.code,httpresponse.reason))
                    return responseJson                 
                except HTTPError as e:
                    _log.warning('HTTPError \'%s : %s\' when requesting measurements data, skipping...' %  (e.code,e.message))
                    pass
                except Exception as e:
                    # Other errors are possible, such as IOError.
                    _log.error("Error: " + str(e))       
            

        return None

class NetatmoHandler(web.RequestHandler):
    executor = ThreadPoolExecutor(max_workers=1)
    def set_extra_headers(self, path):
        # Disable cache
        self.set_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')
        
    @gen.coroutine
    def get(self):
        #global netatmo_auth_token
        #global netatmo_auth_expires
        _log.info('Starting netatmo..')
        _log.info(netatmo_access_token)
        _log.info(netatmo_access_expires)

        if (netatmo_access_token is None or netatmo_access_expires is None or netatmo_access_expires-time.time() < 600):
            _log.info('Starting authentication...')
            yield NetatmoHelper.authenticateNetatmo(self)

        if netatmo_access_token is not None:
            _log.info(netatmo_access_expires)
            _log.info('Using auth token: %s, valid until: %s' % (netatmo_access_token,time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(netatmo_access_expires))))
            q = self.get_argument('q',None)
            pretty = self.get_argument('pretty',None)
            if (pretty == 'true'):
                # template.render() returns a string which contains the rendered html
                # only argument 'q' is passed on 
                self.request.arguments.pop('pretty')
                u = self.request.protocol + "://" + self.request.host + self.request.path +'?' + '&'.join(['%s=%s' % (key, bytes(value[0]).decode('utf-8')) for (key, value) in self.request.arguments.items()]) 
                _log.info('Prettifying request : %s' % u)
                response = template.render(filename=u)
                self.write(response)
            else:
                _log.info('Running \'%s\'-query' % q)
                if (q == 'dash'):
                    response = yield NetatmoHelper.getNetatmoDashboardData(self,netatmo_access_token)
                elif (q == 'getmeasure'):
                    response = yield NetatmoHelper.getNetatmoMeasureData(self,netatmo_access_token)

                if response is not None:
                    self.write('%s' % response)


        self.finish()

class ClimaDataHandler(web.RequestHandler):

    @gen.coroutine
    def get(self):
        q = self.get_argument('q',None)
        tzname = self.get_argument('tz',None)  # UTC or for example Europe/Helsinki
        query = None
        resp = None

        if (q == 'all'):
            table = self.get_argument('table','aClimateDevices')
            query = ('SELECT * FROM %s' % table)
            _log.debug('SQL: \n%s' % query)
            resp = yield DBHelperSQLITE.dbExecute(_dbHelperSQLITE,query)
        elif (q == 'climaDaily'):
            args = {}
            args['datatypes'] = self.get_argument('datatypes','Temperature').split(',') # default Temperature,Humidity other: CO2,Pressure etc
            args['device'] = self.get_argument('device','02-00-00-03-08-1c') # default device Netatmo outdoor module
            loctype = self.get_argument('loctype','outdoor')
            sourceType = self.get_argument('sourceType','netatmo')
            tzname = self.get_argument('tz','UTC') # use UTC if no specific timezone requested

            usecache = self.get_argument('usecache','True')
            readyCaches = yield DBHelperMYSQL.haveCaches(_dbHelperMYSQL,args)
            if usecache == 'True' and readyCaches and len(readyCaches)>0:
                _log.info('Reading from data cache')
                cachename = next (iter (readyCaches.values())) # expecting all data from one cache
                d = _dbHelperMYSQL.dataCaches[cachename]['cache'].tz_localize('UTC').tz_convert(tzname).resample('D',how=['min','max','mean'])
                d.index.names = ['ts']
                d.columns = ['min','max','avg']
                resp = d.to_csv(header=True,date_format='%Y-%m-%d',quoting=csv.QUOTE_NONNUMERIC)
            else:       # failsafe read from db, slow but working
                _log.info('Fallback to db read')
                sqlbuf = StringIO()
                sqlbuf.write('SELECT DATE(CONVERT_TZ(FROM_UNIXTIME(eventNanoTs/1e9),\'UTC\',\'Europe/Helsinki\')) AS ts,\n')
                sqlbuf.write('    MIN(value) AS min,MAX(value) AS max,AVG(value) AS avg\n') 
                sqlbuf.write('    FROM aClimateData\n')
                sqlbuf.write('    WHERE sourceId = \'%s\'\n' % args['device'])
                sqlbuf.write('    AND eventType = \'%s\'\n' % args['datatypes'][0])
                sqlbuf.write('    GROUP BY ts;\n')
                query = sqlbuf.getvalue()

                _log.debug('SQL: \n%s' % query)
                resp = yield DBHelperMYSQL.dbExecute(_dbHelperMYSQL,query,parseresp='CSV')

        elif (q == 'climaEvents'):
            args = {}
            args['datatypes'] = self.get_argument('datatypes','Temperature,Humidity').split(',') # default Temperature,Humidity other: CO2,Pressure etc
            args['device'] = self.get_argument('device','02-00-00-03-08-1c') # default device Netatmo outdoor module
            args['loctype'] = self.get_argument('loctype','outdoor')
            args['sourceType'] = self.get_argument('sourceType','netatmo')
            args['mindate'] = self.get_argument('mindate',None) 
            args['maxdate'] = self.get_argument('maxdate',None) 
            args['tzname'] = self.get_argument('tz','UTC')

            usecache = self.get_argument('usecache','True')

            args = yield parseRequestDates(args)
            mints = args['mints']
            maxts = args['maxts']

            readyCaches = yield DBHelperMYSQL.haveCaches(_dbHelperMYSQL,args)
            if usecache == 'True' and readyCaches and len(readyCaches)>0:
                _log.info('Reading from data caches')
                # csv "ts","Temperature","Humidity" "2015-11-14T22:02:21.000000Z",5.1,94 

                d = None
                for datatype in args['datatypes'] : 
                    cachename = readyCaches[datatype]               
                    # slice out interesting part (mints to maxts) from cache = pd.DataFrame
                    # need to explicitly set sortlevel to 'eventNanoTs' for multi-leveled indexed
                    tmp_df = _dbHelperMYSQL.dataCaches[cachename]['cache'].sortlevel(level='eventNanoTs')[mints.tz_convert('UTC'):maxts.tz_convert('UTC')]
                    # if multi-level index on 'source => filter out (xs - crosssection) only current 'device' (args['device'])
                    if 'sourceId' in tmp_df.index.names:
                        tmp_df = tmp_df.xs([args['device']],level=['sourceId'])
                    # if still multi-level index on 'eventType' => filter out (xs - crosssection) only current 'datatype'
                    if 'eventType' in tmp_df.index.names:
                        tmp_df = tmp_df.xs([datatype],level=['eventType'])
                    if d is None:
                        d = tmp_df
                    else:
                        d = pd.merge(d,tmp_df,left_index=True,right_index=True,how='outer')

                # Should have only one index left (eventTimestamp), rename 
                d.index.names = ['ts']
                d.columns = args['datatypes']
                resp = d.to_csv(header=True,date_format=df,quoting=csv.QUOTE_NONNUMERIC)
            else:
                _log.warning('Data caches not ready or unknown request for request %s' % self.request.uri)
                resp = "Data not available"

        else:
            _log.info('Invalid query')

        self.write(str(resp))
        _log.info('Request done : %.3f ms' % (self.request.request_time()*1000))
        self.finish()

class EnergyDataHandler(web.RequestHandler):

    @gen.coroutine
    def get(self):
        t0 = time.time()
        q = self.get_argument('q',None)

        query = None
        if (q == 'all'):
            query = 'SELECT * FROM aPower'
            _log.debug('SQL: \n%s' % query)
            resp = yield DBHelperSQLITE.dbExecute(_dbHelperSQLITE,query,parseresp='CSV')
        elif (q == 'dailyLast'):
            query = 'select ts,circuit9_cumul,circuit10_cumul,circuit11_cumul,circuit12_cumul,circuit13_cumul,circuit14_cumul,circuit15_cumul,circuit16_cumul from aPower where id in (SELECT max(id) FROM aPower group by date(ts))'
            _log.debug('SQL: \n%s' % query)
            resp = yield DBHelperSQLITE.dbExecute(_dbHelperSQLITE,query,parseresp='CSV')
        elif (q == 'grp'):
            query = 'select datetime((strftime(\'%s\', ts) / 300) * 300, \'unixepoch\') interval, count(*) cnt, max(circuit16_curr) peak, max(circuit16_cumul) reading from aPower group by interval order by interval';
            _log.debug('SQL: \n%s' % query)
            resp = yield DBHelperSQLITE.dbExecute(_dbHelperSQLITE,query,parseresp='CSV')
        elif (q == 'powerstats'):
            args = {}
            args['datatypes'] = self.get_argument('datatypes','all_use,all_cumul,all_peak,measures_count').split(',') # default Temperature,Humidity other: CO2,Pressure etc
            args['device'] = self.get_argument('device','electricity') 
            args['mindate'] = self.get_argument('mindate',None) 
            args['maxdate'] = self.get_argument('maxdate',None) 
            args['tzname'] = self.get_argument('tz','UTC')
            args['aggrto'] = self.get_argument('aggrto',None)
            args['aggrhow'] = self.get_argument('aggrhow','sum')
            args['aggrlabel'] = self.get_argument('aggrlabel','left')

            args = yield parseRequestDates(args)
            mints = args['mints']
            maxts = args['maxts']

            readyCaches = yield DBHelperMYSQL.haveCaches(_dbHelperMYSQL,args)
            if readyCaches and len(readyCaches) > 0 :
                _log.info('Reading from data caches')
                # csv "ts","all_use","all_cumul","all_peak","measures_count" "2016-01-06 00:00:00",45061,191692,860,982 
                cachename = next (iter (readyCaches.values())) # all data in same cache 'energyelectricity'
                # slice out interesting part (mints to maxts) from cache = pd.DataFrame
                # need to explicitly set sortlevel to 'eventNanoTs' for multi-leveled indexed (not mandatory in this case, 'energyelectricity' always one-level index)
                d = _dbHelperMYSQL.dataCaches[cachename]['cache'].sortlevel(level='eventNanoTs')[mints.tz_convert('UTC'):maxts.tz_convert('UTC')]
                
                if 'all_use' in args['datatypes']:  # create summary column for "all_use"
                    cols = ['c1_use','c2_use','c3_use','c4_use','c5_use','c6_use','c7_use','c8_use']
                    d['all_use'] = d[cols].sum(axis=1,skipna=True,numeric_only=True)                   
                if 'all_cumul' in args['datatypes']:  # create summary column for "all_use"
                    cols = ['c1_cumul','c2_cumul','c3_cumul','c4_cumul','c5_cumul','c6_cumul','c7_cumul','c8_cumul']
                    d['all_cumul'] = d[cols].sum(axis=1,skipna=True,numeric_only=True)                   
                if 'all_peak' in args['datatypes']:  # create aggregate max column for "all_peak"
                    cols = ['c1_peak','c2_peak','c3_peak','c4_peak','c5_peak','c6_peak','c7_peak','c8_peak']
                    d['all_peak'] = d[cols].max(axis=1,skipna=True,numeric_only=True)                   

                # filter columns = datatypes array
                d = d[args['datatypes']]
                # set column names to 'datatypes'
                d.columns = args['datatypes']
                # Should have only one index left (eventTimestamp), rename 
                d.index.names = ['ts']
                # resample to requested aggregation,same aggregation rule applied to all columns, aggregation done in tz
                if args['aggrto'] and d.size > 0:
                    if args['tzname'] == 'UTC':
                        d = d.resample(args['aggrto'],how=args['aggrhow'],label=args['aggrlabel'])
                    else: 
                        t1 = d.tz_localize('UTC')
                        _log.debug(t1.info())
                        t2 = t1.tz_convert(args['tzname'])
                        _log.debug(t2.info())
                        t3 = t2.resample(args['aggrto'],how=args['aggrhow'],label=args['aggrlabel'])
                        _log.debug(t3.info())
                        d=t3.tz_convert('UTC')

                resp = d.to_csv(header=True,date_format=df,quoting=csv.QUOTE_NONNUMERIC,na_rep='NaN')
            else:
                _log.warning('Data caches not ready or unknown request for request %s' % self.request.uri)
                resp = "Data not available"
        elif (q == 'energywater'):
            args = {
                'datatypes' : self.get_argument('datatypes','c1_delta').split(','), # default Temperature,Humidity other: CO2,Pressure etc
                'device' : self.get_argument('device','water'), # woodboiler
                'mindate' : self.get_argument('mindate',None), 
                'maxdate' : self.get_argument('maxdate',None), 
                'tzname' : self.get_argument('tz','UTC'),
                'aggrto' : self.get_argument('aggrto',None),
                'aggrhow' : self.get_argument('aggrhow','sum'),
                'aggrlabel' : self.get_argument('aggrlabel','left')
            }
            args = yield parseRequestDates(args)
            readyCaches = yield DBHelperMYSQL.haveCaches(_dbHelperMYSQL,args)
            if readyCaches and len(readyCaches) > 0 :
                _log.info('Reading from data caches')
                cachename = next (iter (readyCaches.values())) # one datatype => all data in same cache 
                # slice out interesting part (mints to maxts) from cache = pd.DataFrame
                d = _dbHelperMYSQL.dataCaches[cachename]['cache'][args['mints'].tz_convert('UTC'):args['maxts'].tz_convert('UTC')]
                d = d[args['datatypes']]  # filter columns = datatypes array
                d.columns = args['datatypes']   # set column names to 'datatypes'
                d.index.names = ['ts']  # Should have only one index left (eventTimestamp), rename 
                if args['aggrto'] and d.size > 0: # resample to requested aggregation,same aggregation rule applied to all columns, aggregation done in tz
                    if args['tzname'] == 'UTC':
                        d = d.resample(args['aggrto'],how=args['aggrhow'],label=args['aggrlabel'])
                    else: 
                        d = d.tz_localize('UTC').tz_convert(args['tzname']).resample(args['aggrto'],how=args['aggrhow'],label=args['aggrlabel']).tz_convert('UTC')
                resp = d.to_csv(header=True,date_format=df,quoting=csv.QUOTE_NONNUMERIC,na_rep='NaN')
            else:
                _log.warning('Data caches not ready or unknown request for request %s' % self.request.uri)
                resp = "Data not available"

        elif (q == 'combined'):
            combo = self.get_argument('combo','indirect0')
            readyCaches = []
            if combo == 'indirect0':  # "direct" = floorheating c1(water) + ventilation c4(electricity) + floorbathroom c3(electricity) + domestic water c3(water)
                args_el = {
                    'datatypes' : 'c4_use,c3_use'.split(','), 
                    'device' : 'electricity'
                }
                args_wat = {
                    'datatypes' : 'c1_delta,c3_delta'.split(','), 
                    'device' : 'water'
                }
                readyCaches = yield DBHelperMYSQL.haveCaches(_dbHelperMYSQL,args_el)
                if readyCaches and len(readyCaches) > 0 :
                    tmp = yield DBHelperMYSQL.haveCaches(_dbHelperMYSQL,args_wat)
                    if tmp and len(tmp) > 0:
                        readyCaches.update(tmp)
                    else:
                        readyCaches = None
                        _log.warning('Water energy cache not available')
                else:
                    _log.warning('Electric energy cache not available')
                datatypes = ['c1_delta','c4_use','c3_use','c3_delta']

            elif combo == 'indirect1': # "indirect" = boiler c6+c7+c8(electricity) + woodboiler c2(water) 
                args_el = {
                    'datatypes' : 'c6_use,c7_use,c8_use'.split(','), 
                    'device' : 'electricity'
                }
                args_wat = {
                    'datatypes' : 'c2_delta'.split(','), 
                    'device' : 'water'
                }
                readyCaches = yield DBHelperMYSQL.haveCaches(_dbHelperMYSQL,args_el)
                if readyCaches and len(readyCaches) > 0 :
                    tmp = yield DBHelperMYSQL.haveCaches(_dbHelperMYSQL,args_wat)
                    if tmp and len(tmp) > 0 :
                        readyCaches.update(tmp)
                    else:
                        readyCaches = None
                        _log.warning('Water energy cache not available')
                else:
                    _log.warning('Electric energy cache not available')

                datatypes = ['c6_use','c7_use','c8_use','c2_delta']
            else:
                _log.info('Invalid combination requested')

            args = {
                'mindate' : self.get_argument('mindate',None), 
                'maxdate' : self.get_argument('maxdate',None), 
                'tzname' : self.get_argument('tz','UTC'),
                'aggrto' : self.get_argument('aggrto',None),
                'aggrhow' : self.get_argument('aggrhow','sum'),
                'aggrlabel' : self.get_argument('aggrlabel','left')
            }
            args = yield parseRequestDates(args)

            if readyCaches and len(readyCaches) > 0 :
                _log.info('Reading from data caches')

                d = None
                for datatype in datatypes : 
                    cachename = readyCaches[datatype]               
                    # slice out interesting part (mints to maxts) from cache = pd.DataFrame
                    # need to explicitly set sortlevel to 'eventNanoTs' for multi-leveled indexed
                    tmp_df = _dbHelperMYSQL.dataCaches[cachename]['cache'][args['mints'].tz_convert('UTC'):args['maxts'].tz_convert('UTC')]
                    if d is None:
                        d = tmp_df[[datatype]]  # selecting single column as new DataFrame, double brackets needed otherwise d becomes a Series and not a DataFrame
                    else:
                        d = pd.merge(d,tmp_df[[datatype]],left_index=True,right_index=True,how='outer')

                # Should have only one index left (eventTimestamp), rename 
                d.index.names = ['ts']
                d.columns = datatypes

                if combo == 'indirect1': # sum c6,c7,c8 (3-phase) to c678_use
                    d['c678_use'] = d[['c6_use','c7_use','c8_use']].sum(axis=1,numeric_only=True)
                    d.drop(['c6_use','c7_use','c8_use'],axis=1,inplace=True)  # remove c6-c8
                    d = d.ix[:,['c678_use','c2_delta']] # switch places c678 first, c2_delta second column

                # resample to requested aggregation,same aggregation rule applied to all columns, aggregation done in tz
                if args['aggrto'] and d.size > 0:
                    if args['tzname'] == 'UTC':
                        d = d.resample(args['aggrto'],how=args['aggrhow'],label=args['aggrlabel'])
                    else: 
                        d = d.tz_localize('UTC').tz_convert(args['tzname']).resample(args['aggrto'],how=args['aggrhow'],label=args['aggrlabel']).tz_convert('UTC')

                resp = d.to_csv(header=True,date_format=df,quoting=csv.QUOTE_NONNUMERIC)

            else:
                _log.warning('Data caches not ready or unknown request for request %s' % self.request.uri)
                resp = "Data not available"

        else:
            _log.info('Invalid query')

        self.write(str(resp))
        _log.info('Request done in %.3fs' % (time.time()-t0))
        self.finish()

class BackgroundTaskHandler(web.RequestHandler):

    @gen.coroutine
    def get(self):
        global dbAggregation_running,bgloop,dbCache_update

        turn = self.get_argument('turn',None)
        if (turn == 'on'):
            dbAggregation_running = True
            _log.info('Turn data aggregation \'on\', AggregationRunning : %s' % dbAggregation_running)
        elif (turn == 'off'):
            dbAggregation_running = False
            _log.info('Turn data aggregation \'off\', AggregationRunning : %s' % dbAggregation_running)
    
        loglvl = self.get_argument('loglevel',None)
        if loglvl and len(loglvl) > 0:
            lvl = logging.getLevelName(loglvl)
            if lvl:
                try:
                    _log.info('Setting log-level to  : %s' % logging.getLevelName(lvl))
                    _log.setLevel(lvl)
                except Exception:
                    _log.warning('Incorrect log level \'%s\', loglevel unchanged' % lvl)

        bgl = self.get_argument('bgloop',None)
        if bgl and float(bgl) > 10.0:  # safety margin, shortest loop 10secs
            bgloop = float(bgl)

        cache = self.get_argument('cache',None)
        if cache and len(cache) > 0:
            _cacheupdstack.append(cache)
            _log.info('Triggered cache update for next bg loop : %s' % cache)

        _log.info('Effective BG parameters:  AggregationRunning : %s, Log Level : %s BGloop : %f seconds' % (dbAggregation_running,(dbAggregation_running,logging.getLevelName(_log.getEffectiveLevel())),bgloop))

        aggr = self.get_argument('aggr',None)
        if aggr is not None:
            if (aggr == 'mbus'):
                result = yield DBAggregator.aggregateMC302()
            elif (aggr == 'power'):
                result = yield DBAggregator.aggregatePower()
            else:
                _log.info('Bad aggr request : %s' % aggr)

        self.write('<html><body><div>BG LOOP RUNNING : %s</div><div>LOG_LEVEL : %s</div></body></html>' % (dbAggregation_running,logging.getLevelName(_log.getEffectiveLevel())))
        self.finish()


# Handler for main page
class MainHandler(web.RequestHandler):

    def get(self):
        # Returns rendered template string to the browser request
        _log.info("In main handler")
        self.write('OK')

# Assign handler to the server root  (127.0.0.1:PORT/)
application = web.Application([
    (r"/", MainHandler),
    (r'/static/(.*)', web.StaticFileHandler, {'path': static_path}),
    (r'/ahouse/(.*)', web.StaticFileHandler, {'path': ahouse_path}),
    (r"/bg",BackgroundTaskHandler),
    (r"/clima",ClimaDataHandler),
    (r"/netatmo",NetatmoHandler),
    (r"/power", EnergyDataHandler)
])
PORT=8889
if __name__ == "__main__":
    import logging
    logFormat = '%(asctime)s:%(levelname)s:%(funcName)s:%(message)s'
    logging.basicConfig(format=logFormat)
    
#    options.options['log_file_prefix'].set('/opt/logs/my_app.log')
    #options.parse_command_line() # read the command-line to get log_file_prefix from there
    _log = logging.getLogger("tornado.application")
    _log.info('\n--------------------------------------------------------------------------\nApplication starting')
    _log.info('Setting log-level : %s',logging.getLevelName(LOG_LEVEL))
    _log.setLevel(LOG_LEVEL)   # 50 critical, 40 error, 30 warning, 20 info, 10 debug

    setupsql() # Prepare sqls
    # Setup the server
    application.listen(PORT)

    _dbHelperMYSQL = DBHelperMYSQL()
    _scheduler = TornadoScheduler()
    _scheduler.start()
    ioloop = IOLoop.instance()

    bgtask = TaskRunner(ioloop)
    TaskRunner.setupbg(bgtask)

    ioloop.start()