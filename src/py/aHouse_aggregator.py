# import Tornado
from tornado.ioloop import IOLoop
from tornado import gen
from tornado import web
from tornado import options
from tornado.httpclient import AsyncHTTPClient,HTTPRequest,HTTPError
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
import tornado.escape
import requests

# DB
import sqlite3

# Date and time
from datetime import datetime,timezone
import pytz
from tzlocal import get_localzone
import time
from apscheduler.schedulers.tornado import TornadoScheduler

# M-Bus
from mbus.MBus import MBus

# Parsing
import json
import csv
from io import StringIO
import xmltodict

# Other
import os
import codecs
#import sys
#import pprint


LOG_LEVEL = 20 # 50 critical, 40 error, 30 warning, 20 info, 10 debug

dbAggregation_running = True # start on server startup
dbAggregation_runonce = False

basedir='./'
static_path = basedir+'src/web/html/'
appdir = basedir+'src/py/'
dbPath = basedir+'db/'
dbFile = dbPath+'aHouse_Measurements.sqlite'
dbConn = None
dbCursor = None
_dbHelper = None

_scheduler = None

dt0 = datetime.utcfromtimestamp(0)  # epoch as naive datetime object without timezone
dt0_with_tz = datetime.utcfromtimestamp(0).replace(tzinfo=timezone.utc)  # epoch as datetime object with timezone (utc)
bgloop_syncto = 60.0 # even seconds to sync to, called once when app starts

mbusSerialPort = b'/dev/ttyUSB0' 
# 2015-11-10T07:46:13.771123Z 
df = '%Y-%m-%dT%H:%M:%S.%fZ'  # datetime format used in db, always UTC time, microsecond -> millisecond stripping at interfaces

# sql filenames: <dbpath> + key + '.sql'
# sql_statements = {'create_table_aPower':''
#     ,'select_aPowerStats_all':''
#     }
sql_statements = {}

# DB helper function 
# def _setupsql():
# #    global dbConn,dbCursor
#     dbConn = sqlite3.connect(dbFile)
#     dbCursor = dbConn.cursor()
#     _log.info('Setup db, creating table if not exist')
#     # with open('create_table_aPower.sql','r',encoding='utf-8') as sql_file:
#     #     createtable = sql_file.read() 
#     #     dbCursor.executescript(createtable)
#     # with open('create_table_aClimate.sql','r',encoding='utf-8') as sql_file:
#     #     createtable = sql_file.read() 
#     #     dbCursor.executescript(createtable)
#     # dbConn.commit()
#     _log.info('Loading sql-statements from files','Normal')
#     for key in sql_statements:
#         with open(key+'.sql', 'r', encoding='utf-8') as sql_file:
#             sql_statements[key] = sql_file.read()
#         log('file : %s sql : \n%s' % (key,sql_statements[key]),'Debug')
#     return

# def _execute(query):
#     """Function to execute queries against a local sqlite database"""
#     try:
#             dbCursor.execute(query)

#             result = dbCursor.fetchall()
#             dbConn.commit()
#     except Exception:
#             raise
#     dbCursor.close()
#     return result

class DBHelperSQLITE(object):
    def __init__(self):
        self.dbexecutor = ThreadPoolExecutor(1)
        self.dbConn = None
        self.dbCursor = None

    @run_on_executor(executor='dbexecutor')
    def dbConnect(self):
        _log.info('Setting up database connection')
        self.dbConn = sqlite3.connect(dbFile,check_same_thread=True) 
        self.dbCursor = self.dbConn.cursor()
        with open(dbPath+'create_table_aMbus.sql','r',encoding='utf-8') as sql_file:
            createtable = sql_file.read() 
            self.dbCursor.executescript(createtable)
            self.dbConn.commit()
        with open(dbPath+'create_table_aMbusMC302Record.sql','r',encoding='utf-8') as sql_file:
            createtable = sql_file.read() 
            self.dbCursor.executescript(createtable)
            self.dbConn.commit()
        with open(dbPath+'create_table_aPower.sql','r',encoding='utf-8') as sql_file:
            createtable = sql_file.read() 
            self.dbCursor.executescript(createtable)
            self.dbConn.commit()
        self.dbCursor.execute('SELECT sqlite_version(),sqlite_source_id();')
        ver = self.dbCursor.fetchone()
        _log.info('Data connection opened. SQLite version %s %s' % (ver[0],ver[1])) 
        return 

    @gen.coroutine
    def dbExecute(self,query,parseresp=None,tzname=None,script=False,returnrowcount=False):
        if not self.dbConn:
            _log.debug('Connecting to db')
            res = yield self.dbConnect()
            _log.debug('Connected to db')

        res = yield DBHelperSQLITE._dbExecute(self,query,parseresp,tzname,script,returnrowcount)
        return res

    @run_on_executor(executor='dbexecutor')
    def _dbExecute(self,query,parseresp,tzname,script,returnrowcount):
        """Function to execute queries against a local sqlite database"""
        _log.debug('Executing SQL')
        try:
            if tzname is not None:
                _log.info('Doing timzone conversion to timezone \'%s\'' % tzname)
                # utilizing db localtime function by setting timezone to requested tz, not threadsafe!!
                saved_tz = get_localzone().zone # save current timezone so it can be reset after query
                os.environ['TZ'] = tzname       # set environment variable to requested timezone
                time.tzset()                    # update timezone from env
                if script:
                    self.dbCursor.executescript(query)
                else:
                    self.dbCursor.execute(query)
                os.environ['TZ'] = saved_tz     # reset env to saved timezone
                time.tzset()                    # update timezone from env
            else:
                _log.debug('No timezone conversion')
                if script:
                    self.dbCursor.executescript(query)
                else:
                    self.dbCursor.execute(query)

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
                    _log.debug('Db rowcount :\n%d' % result)
                else:
                    result = self.dbCursor.fetchall()
                    _log.debug('DB result :\n%s' % result)

            self.dbConn.commit()
        except Exception:
            raise
        #dbCursor.close()
        return result

class PowerPoller(object):
    @gen.coroutine
    def getPowerData():

        url = "http://192.168.12.15/csv.html"
        #dbfile = "aHouseEnergy.sqlite"
        #list of elementpaths to ignore in comparison
        # discard_elem = ["path/to/elem",
        #                   "path/tpo/another/elem"] 
        #discard_elem = ["path/to/elem"] 
        include_elem = [9,10,11,12,13,14,15,16,18,19,20,21,22,23,24,25] 
        polling_interval = 5.0 # (in seconds)
        # oldstats = None
        # newstats = None
        # chglog = ''
        _log.debug('Polling for power data')

        datastartdelim = "<body>"
        dataenddelim ="<br>\n</body>"
  
        # flagchanged = 'NoChange'
        http_request = HTTPRequest(url)   
        http_client = AsyncHTTPClient()

        try:
            httpresponse = yield http_client.fetch(http_request)
            #response = requests.get(url, timeout=(3.05,3))  # get data, connect timeout 2sec, read timeout 3sec
            response = httpresponse.body.decode("utf-8")
            _log.debug('Retrieved response: \n%s' % response)
            # find parse data element
            if len(response) == 0:
                _log.info("No data retrieved")
            else:
                startpos = response.find(datastartdelim) + len(datastartdelim)
                endpos = response.find(dataenddelim)
                _log.debug("Start pos %d - end pos %d" % (startpos,endpos))
                _log.debug('csv string \'%s\'' % (response[startpos:endpos]))
                data = response[startpos:endpos].split(',')

                sqlbuf = StringIO()
                sqlbuf.write("INSERT INTO aPower VALUES (null,\'%s\'" % (datetime.utcnow()))
                for i in range(9,17):  # cumulative data in (9-16)
                    if i in include_elem:
                        sqlbuf.write(',\'%s\'' % data[i])
                    else:
                        sqlbuf.write(',null')
                for i in range(18,26):  # current data in (18-25)
                    if i in include_elem:
                        sqlbuf.write(',\'%s\'' % data[i])
                    else:
                        sqlbuf.write(',null')
                sqlbuf.write(')')

                _log.debug('---- SQL Start ----\n%s\n---- SQL End----\n' % sqlbuf.getvalue())

                result = []
                try:
                    result = yield DBHelperSQLITE.dbExecute(_dbHelper,sqlbuf.getvalue(),parseresp=None,script=False,returnrowcount=False)
                except Exception as e:
                    _log.warning('Error during database operation : %s' % str(e))
                    raise


        except requests.exceptions.Timeout:
            _log.warning("\nRead timeout, skipping..")
            raise
        except Exception as e:
            #if log_level in ('High','Debug'):
            _log.warning(str(e))           # __str__ allows args to be printed directly
            _log.warning("\nOther exception than timeout, still not breaking..")
            raise
        _log.info('Power poll done')

class TaskRunner(object):
    def __init__(self, loop):
        self.mbusexecutor = ThreadPoolExecutor(1) # separate thread for MBus operations over serial interface (slow)
        self.loop = loop
        self.arguments = {}
        self.mbusConn = None

    def __del__(self):
        self.closeMbusConnection()

    def get_argument(self, name, default, strip=True):  # mimicing HTTPRequestHander object make function usable both on  HTTPRequestHandler- and TaskRunner-objects
        return self.arguments[name] if name in self.arguments else default

    @gen.coroutine
    def bgheartbeat():
        out = StringIO() 
        _scheduler.print_jobs(out=out)
        _log.info('%s' % out.getvalue())

    @gen.coroutine
    def bgsetup(self):
        firstrun = datetime.fromtimestamp(divmod(datetime.now().timestamp(),bgloop_syncto)[0]*bgloop_syncto+bgloop_syncto)
        _log.info('Background jobs first run at %s' % firstrun)

        _scheduler.add_job(TaskRunner.bgheartbeat, 'interval', seconds=60,next_run_time=firstrun)
        _scheduler.add_job(TaskRunner.readAndStoreMbusData, 'interval',args=[self],seconds=60,next_run_time=firstrun)
        _scheduler.add_job(PowerPoller.getPowerData, 'interval',seconds=5,next_run_time=firstrun)

        out = StringIO() 
        _scheduler.print_jobs(out=out)
        _log.info('%s' % out.getvalue())
        return

    @gen.coroutine
    def dbUpsertMbusSlaveInfo(self,slaveinfo,addr):
        _log.info('Updating (or inserting) mbus slave metadata to database') 
        result = 0
        sqlbuf = StringIO()
        sqlbuf.write('UPDATE OR IGNORE aMbusSlaveInfo SET\n')   # update fails intentionally when row not existing, then do insert 
        sqlbuf.write('   manufacturer = \'%s\',\n' % slaveinfo['Manufacturer'])
        sqlbuf.write('   version = %d,\n' % int(slaveinfo['Version']))
        sqlbuf.write('   productName = \'%s\',\n' % slaveinfo['ProductName'])
        sqlbuf.write('   medium = \'%s\',\n' % slaveinfo['Medium'])
        sqlbuf.write('   accessNumber = %d,\n' % int(slaveinfo['AccessNumber']))
        sqlbuf.write('   signature = \'%s\',\n' % slaveinfo['Signature'])
        sqlbuf.write('   lastStatus = \'%s\',\n' % slaveinfo['Status'])
        sqlbuf.write('   rowUpdatedTimestamp = \'%s\'\n' % datetime.utcnow().strftime(df))
        sqlbuf.write('WHERE address = %d AND id = %s' % (addr,slaveinfo['Id']))
        _log.debug('SQL:\n%s' % sqlbuf.getvalue())
        #cur.execute(sqlbuf.getvalue())
        try:
            result = yield DBHelperSQLITE.dbExecute(_dbHelper,sqlbuf.getvalue(),parseresp=None,script=False,returnrowcount=True)
        except Exception as e:
            _log.warning('Error during database write : %s' % str(e))
            pass

        if not result: # no rows where updated so insert instead
            _log.warning("Inserting new row for mbus slave metadata, should only happen when data is recreated")
            sqlbuf.truncate(0)
            sqlbuf.seek(0)
            sqlbuf.write('INSERT INTO aMbusSlaveInfo(address,id,manufacturer,version,productName,medium,accessNumber,signature,lastStatus,rowCreatedTimestamp)\n')
            sqlbuf.write('  VALUES (%d,\'%s\',\'%s\',%d,\'%s\',\'%s\',%d,\'%s\',\'%s\',\'%s\');\n' % (addr,slaveinfo['Id'],slaveinfo['Manufacturer'],int(slaveinfo['Version']),slaveinfo['ProductName'],slaveinfo['Medium'],int(slaveinfo['AccessNumber']),slaveinfo['Signature'],slaveinfo['Status'],datetime.utcnow().strftime(df)))
            _log.debug('SQL:\n%s' % sqlbuf.getvalue())
            #cur.execute(sqlbuf.getvalue())
            try:
                result = yield DBHelperSQLITE.dbExecute(_dbHelper,sqlbuf.getvalue(),parseresp=None,script=False,returnrowcount=True)
            except Exception as e:
                _log.warning('Error during database write : %s' % str(e))
                pass

            #self.bgDbConn.commit()
        return result;

    @gen.coroutine
    def dbInsertMbusDataRecords(self,mbusdata,addr):
        _log.info('Inserting mbus slave data records to database') 
        sqlbuf = StringIO()
        sqlbuf2 = StringIO()
        s2len = 0
        slaveid = mbusdata['SlaveInformation']['Id']
        accessnum = mbusdata['SlaveInformation']['AccessNumber']
        mbusdf = '%Y-%m-%dT%H:%M:%S' # 2016-01-28T06:48:13 (In UTC)
        creationtime = datetime.utcnow().strftime(df) 
        result = 0
        for record in mbusdata['DataRecord']:
            _log.debug('Inserting mbus slave data records (address = %d , id = %d) to database' % (addr,int(record['@id']))) 

            sqlbuf.write('INSERT INTO \
                aMbusDataRecord(address,id,recordId,recordFunction,recordStorageNumber,recordUnit,recordValue,recordTimestampRaw,recordTimestamp,rowCreatedTimestamp,accessNumber)\n')
            sqlbuf.write('  VALUES (%d,\'%s\',%d,\'%s\',%d,\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%d);\n' % \
                (addr,slaveid,int(record['@id']),record['Function'],int(record['StorageNumber']),record['Unit'],record['Value'],record['Timestamp'],datetime.strptime(record['Timestamp'],mbusdf).replace(tzinfo=timezone.utc).strftime(df),datetime.utcnow().strftime(df),int(accessnum)))

            optionals = {k: record.get(k,None) for k in ('@frame','Tariff','Device') if k in record}  # checking to see if optional data available in record
            if len(optionals):
                m = {'@frame':'recordFrame','Tariff':'recordTariff','Device':'recordDevice'}
                _log.debug('Mbus response contains optionals: %s' % optionals)
                s2len += sqlbuf2.write('UPDATE aMbusDataRecord SET')
                for i,key in enumerate(optionals):
                    if i != 0: s2len += sqlbuf2.write(' AND')
                    s2len += sqlbuf2.write(' %s = \'%s\'' % (m[key],optionals[key]))
                s2len += sqlbuf2.write(' WHERE address=%d AND id=\'%s\' AND recordId=\'%s\' AND recordTimestamp=\'%s\';\n' % (addr,slaveid,record['@id'],creationtime))
            
        _log.debug('SQL:\n%s' % sqlbuf.getvalue())

        #cur.executescript(sqlbuf.getvalue())
        try:
            result = yield DBHelperSQLITE.dbExecute(_dbHelper,sqlbuf.getvalue(),parseresp=None,script=True,returnrowcount=True)
        except Exception as e:
            _log.warning('Error during database write : %s' % str(e))
            pass

        if s2len > 0: 
            _log.debug('SQL(Optionals):\n%s' % sqlbuf2.getvalue())
            #cur.executescript(sqlbuf2.getvalue())
            try:
                result = yield DBHelperSQLITE.dbExecute(_dbHelper,sqlbuf2.getvalue(),parseresp=None,script=True,returnrowcount=True)
            except Exception as e:
                _log.warning('Error during database write : %s' % str(e))
                pass
            

        #self.bgDbConn.commit()
        return result;

    @gen.coroutine
    def readAndStoreMbusData(self):
        mbus_addresses = [1,15,31] # m-bus primary addresses to request data from
        # mbus_addresses = [1] # m-bus primary addresses to request data from
        reply_dict = {}
        if not self.mbusConn:
            _log.info('M-bus connnection not opened previously, opening now..')
            self.mbusConn = yield self.openMbusConnection()
        for addr in mbus_addresses:
            try:
                res = yield self.readMbus(addr)
                _log.debug('address: %d resp:\n%s' % (addr,json.dumps(res, indent=4)))
                if res:
                    reply_dict[addr] = res   
                    rowsUpdated = yield self.dbUpsertMbusSlaveInfo(reply_dict[addr]['MBusData']['SlaveInformation'],addr)
                    if rowsUpdated: # only try to add records if previous sql was successful
                        rowsUpdated = yield self.dbInsertMbusDataRecords(reply_dict[addr]['MBusData'],addr)
            except Exception as e:
                _log.error("Exception in m-bus read or data insert: addr: %s, type %s, desc: %s" % (addr,type(e),str(e)))
                pass 
        if len(reply_dict):
            _log.info('Successful m-bus read, aggregating MC302 records')
            res = yield self.aggregateMC302DataRecords()
            # for k,resp in enumerate(reply_dict):
            #     _log.info("i: %s key:%s value:\n%s" % (k,resp,reply_dict[resp]))
        else:
            _log.info('No data received over m-bus')

        #mbusConn = yield self.closeMbusConnection()  # FIX: don't close all the time
        return

    @run_on_executor(executor='mbusexecutor')
    def openMbusConnection(self):
        try:
            _log.info('Opening m-bus connection on device \'%s\'' % mbusSerialPort)
            self.mbusConn = MBus(device=mbusSerialPort) 
            self.mbusConn.connect()
        except Exception as e:
            _log.error("Exception while connecting to m-bus : type %s, desc: %s" % (type(e),str(e)))
            raise Exception(e)
        return self.mbusConn

    @run_on_executor(executor='mbusexecutor')
    def closeMbusConnection(self):
        try:
            _log.info('Closing m-bus connection')
            self.mbusConn.disconnect()
            self.mbusConn = None
        except Exception as e:
            _log.error("Exception while closing connection to m-bus : type %s, desc: %s" % (type(e),str(e)))
            raise Exception(e)
        return self.mbusConn

    @run_on_executor(executor='mbusexecutor')
    def readMbus(self,addr=0):
        reply = None
        try:
            self.mbusConn.send_request_frame(addr)
            reply = xmltodict.parse(self.mbusConn.frame_data_xml(self.mbusConn.frame_data_parse(self.mbusConn.recv_frame()))) # xmltodict works directly on the byte-string xml from libmbus
        except Exception as e:
            _log.error("Exception while requesting data from m-bus : mbus-address: %d, type %s, desc: %s" % (addr,type(e),str(e)))
            raise Exception(e)
        return reply

    def getFactor(self,exp=''):
        factor = 1.0
        if exp == 'm':
            factor = float(1/1000)
        elif exp == 'my':
            factor = float(1/1000000)
        elif exp == '10':
            factor = float(10)
        elif exp == '100':
            factor = float(100)
        elif exp == 'k':
            factor = float(1000)
        elif exp == '10 k':
            factor = float(10000)
        elif exp == '100 k':
            factor = float(100000)
        elif exp == 'M':
            factor = float(1000000)
        elif exp == 'T':
            factor = float(1000000000)
        elif exp.startswith('1e'):
            factor = float(exp)
        return factor

    @gen.coroutine
    def aggregateMC302DataRecords(self):
        # merge 30 mbus data record rows into 1 row and update SlaveInfo 
        _log.info('Aggregating mc302 records...')
        _log.debug('Get timestamp of latest MC302 record update...')

        # Find all mbus slaves and their addresses
        sql = 'select distinct id,address from aMbusSlaveInfo';
        result = []
        try:
            result = yield DBHelperSQLITE.dbExecute(_dbHelper,sql,parseresp=None,script=False,returnrowcount=False)
        except Exception as e:
            _log.warning('Error during database operation : %s' % str(e))
            pass
        slaves = {}
        for row in result:
            slaves[row[0]] = {}
            slaves[row[0]]['address'] = row[1]
        # Find all last aMbusMC302Record-update for the slaves
        sql = 'select id,max(recordTimestamp) from aMbusMC302Record group by id';
        result = []
        try:
            result = yield DBHelperSQLITE.dbExecute(_dbHelper,sql,parseresp=None,script=False,returnrowcount=False)
        except Exception as e:
            _log.warning('Error during database operation : %s' % str(e))
            pass
        for row in result:
            _log.info(row)
            if slaves[row[0]]: slaves[row[0]]['lastUpdate'] = row[1]

        sqlbuf = StringIO()

        #Loop over slave aggregating data
        for id,val in slaves.items():
                # TABLE aMbusDataRecord 
                # rowid INTEGER PRIMARY KEY NOT NULL,
                # address INTEGER NOT NULL,       /* Mbus primary address*/
                # id TEXT NOT NULL,            /* Mbus slave (SlaveInformation) id*/
                # recordId INTEGER NOT NULL,      /* XML: /MBusData/Record@id */
                # recordFrame TEXT,               /* XML: /MBusData/Record@frame (optional), not clear when this data is received but libmbus xml has reserved space for this */ 
                # recordFunction TEXT NOT NULL,   /* XML: /MBusData/Record/Function */
                # recordStorageNumber INTEGER,    /* XML: /MBusData/Record/StorageNumber */
                # recordTariff TEXT,              /* XML: /MBusData/Record/Tariff (optional) */ 
                # recordDevice TEXT,              /* XML: /MBusData/Record/Device (optional) */ 
                # recordUnit TEXT NOT NULL,       /* XML: /MBusData/Record/Unit */ 
                # recordValue INTEGER NOT NULL,   /* XML: /MBusData/Record/Value */ 
                # recordTimestampRaw TEXT NOT NULL,   /* XML: /MBusData/Record/Timestamp */ 
                # recordTimestamp TEXT NOT NULL,      /* Parsed from /MBusData/Record/Timestamp */ 
                # rowCreatedTimestamp INTEGER NOT NULL, 
                # accessNumber INTEGER,
            sql = 'SELECT distinct id,address,recordTimestamp,accessNumber FROM aMbusDataRecord WHERE id = \'%s\'' % id
            if 'lastUpdate' in val: sql += ' AND recordTimestamp > \'%s\'' % val['lastUpdate']
            sql += ';\n'
            timestamps = []
            try:
                timestamps = yield DBHelperSQLITE.dbExecute(_dbHelper,sql,parseresp=None,script=False,returnrowcount=False)
            except Exception as e:
                _log.warning('Error during database operation : %s' % str(e))
                pass

            creationtime = datetime.utcnow().strftime(df) 
            for ts in timestamps:
                curr_id = ts[0]
                curr_addr = ts[1]
                curr_ts = ts[2]
                curr_acc= ts[3]
                
                # TABLE aMbusMC302Record
                # rowId INTEGER PRIMARY KEY NOT NULL,
                # accessNumber INTEGER NOT NULL,  /* Slave MBus response sequence number */
                # address INTEGER NOT NULL,       /* Mbus primary address*/
                # id TEXT NOT NULL,               /* Mbus slave (SlaveInformation) id*/
                # recordTimestampRaw TEXT,    /* same for all data records in single Mbus response*/
                # recordTimestamp TEXT,       /* parsed value of raw */
                # heatingEnergy INTEGER,      /* Heating energy (cumulative, non-resettable) converted to Wh from     1|Instantaneous value|0|Energy;100;Wh */
                # coolingEnergy INTEGER,      /* Cooling energy (cumulative, non-resettable) converted to Wh from     2|Instantaneous value|0|Energy;100;Wh */
                # energyM3T1 INTEGER,     /* Energy mˆ3 * T1 (cumulative Volume * Temperature) from       3|Instantaneous value|0|Manufacturer specific */
                # energyM3T2 INTEGER,     /* Energy mˆ3 * T2 (cumulative Volume * Temperature) from       4|Instantaneous value|0|Manufacturer specific   */
                # volume INTEGER,         /* Current volume (cumulative converted to dm3) from            5|Instantaneous value|0|Volume;m;m^3 */
                # hourCounter INTEGER,        /* Hour counter (non-resettable) from                   6|Instantaneous value|0|On time (hours) */
                # errorHourCounter INTEGER,   /* Error hour counter (cumul hours in error, no resettable) from    7|Value during error state|0|On time (hours) */
                # temp1 REAL,         /* Current (Flow) Temperature T1 (converted to deg.Celsius) from    8|Instantaneous value|0|Flow temperature;1e-2;deg C */
                # temp2 REAL,         /* Current (Return) Temperature T2 (converted to deg.Celsius) from  9|Instantaneous value|0|Return temperature;1e-2;deg C */
                # deltaT1T2 REAL,         /* Temperature difference T1-T2 (converted to  deg.Celsius) from    10|Instantaneous value|0|Temperature Difference;1e-2;deg C */
                # power INTEGER,          /* Current power (converted to W) from                  11|Instantaneous value|0|Power;100;W */
                # powerMax INTEGER,       /* Maximum power since XX (converted to Watt) from          12|Maximum value|0|Power;100;W */
                # flow INTEGER,           /* Current water flow (converted to dm3/h) from             13|Instantaneous value|0|Volume flow;m;m^3/h  */
                # flowMax INTEGER,        /* Maximum water flow since XX (converted to dm3/h)         14|Maximum value|0|Volume flow;m;m^3/h */
                # errorFlags TEXT,        /* Error flags from                         15|Instantaneous value|0|Error flags */
                # timePoint TEXT,         /* Date+Time from                           16|Instantaneous value|0|Time Point (time & date) */
                # targetHeatingEnergy INTEGER,/* Heating energy since targetTimepoint (in Wh) from            17|Instantaneous value|1|Energy;100;Wh */
                # targetCoolingEnergy INTEGER,    /* Cooling energy since targetTimepoint (in Wh) from            18|Instantaneous value|1|Energy;100;Wh */
                # targetEnergyM3T1 INTEGER,   /* Energy mˆ3 * T1 since targetTimepoint from               19|Instantaneous value|1|Manufacturer specific */
                # targetEnergyM3T2 INTEGER,   /* Energy mˆ3 * T2 since targetTimepoint from               20|Instantaneous value|1|Manufacturer specific */
                # targetVolume INTEGER,       /* Volume since targetTimepoint (in dm3) from               21|Instantaneous value|1|Volume;m;m^3 */
                # targetPowerMax INTEGER,     /* Max power since targetTimepoint (in Watt) from           22|Maximum value|1|Power;100;W  */
                # targetFlowMax INTEGER,      /* Max flow since targetTimepoint (in dm3/h) from           23|Maximum value|1|Volume flow;m;m^3/h  */
                # targetTimepoint TEXT,       /* Target time point (date) from                    24|Instantaneous value|1|Time Point (date) */
                # rowCreatedTimestamp TEXT,
                # UNIQUE (accessNumber,id,recordTimestamp) ON CONFLICT REPLACE

                # Insert row
                sql = 'INSERT INTO aMbusMC302Record(id,address,recordTimestamp,accessNumber,rowCreatedTimestamp) VALUES (\'%s\',%d,\'%s\',%d,\'%s\');\n' % (curr_id,curr_addr,curr_ts,curr_acc,creationtime)
                try:
                    result = yield DBHelperSQLITE.dbExecute(_dbHelper,sql,parseresp=None,script=False,returnrowcount=True)
                except Exception as e:
                    _log.warning('Error during database operation : %s' % str(e))
                pass
                _log.debug(sql)

                sql = 'SELECT address,id,recordId,recordStorageNumber,recordUnit,recordValue FROM aMbusDataRecord WHERE id = \'%s\' AND recordTimestamp = \'%s\';\n' % (curr_id,curr_ts)
                records = []
                try:
                    records = yield DBHelperSQLITE.dbExecute(_dbHelper,sql,parseresp=None,script=False,returnrowcount=False)
                except Exception as e:
                    _log.warning('Error during database operation : %s' % str(e))
                pass

                # reuse sqlbuf, empty it first truncate+seek
                sqlbuf.truncate(0) 
                sqlbuf.seek(0)
                tmpbuf = StringIO()
                for record in records:
                    tmpbuf.truncate(0)
                    tmpbuf.seek(0)
                    curr_recid = record[2]
                    curr_recstor = record[3]
                    curr_recunit = record[4]
                    curr_value = record[5]
                    factor = 1.0
                    if ';' in curr_recunit:
                        parts = curr_recunit.split(';')
                        factor = self.getFactor(str(parts[1]))

                    tmpbuf.write('UPDATE aMbusMC302Record SET \n')
                    if curr_recid == 0: # skip "Firmware"
                        _log.debug('Skipping 0 = Firmware for now')
                        continue
                    elif curr_recid == 1: #
                        tmpbuf.write('heatingEnergy = %d\n' % int(factor*curr_value)) 
                    elif curr_recid == 2: # 
                        tmpbuf.write('coolingEnergy = %d\n' % int(factor*curr_value)) 
                    elif curr_recid == 3: # 
                        tmpbuf.write('energyM3T1 = %d\n' % int(curr_value)) 
                    elif curr_recid == 4: # 
                        tmpbuf.write('energyM3T2 = %d\n' % int(curr_value)) 
                    elif curr_recid == 5: # 
                        tmpbuf.write('volume = %d\n' % int(factor*curr_value*1000))   # volume converted from m3 to dm3 
                    elif curr_recid == 6: # 
                        tmpbuf.write('hourCounter = %d\n' % int(curr_value)) 
                    elif curr_recid == 7: # 
                        tmpbuf.write('errorHourCounter = %d\n' % int(curr_value)) 
                    elif curr_recid == 8: # 
                        tmpbuf.write('temp1 = %f\n' % float(factor*curr_value)) 
                    elif curr_recid == 9: # 
                        tmpbuf.write('temp2 = %f\n' % float(factor*curr_value)) 
                    elif curr_recid == 10: # 
                        tmpbuf.write('deltaT1T2 = %f\n' % float(factor*curr_value)) 
                    elif curr_recid == 11: # 
                        tmpbuf.write('power = %d\n' % int(factor*curr_value)) 
                    elif curr_recid == 12: # 
                        tmpbuf.write('powerMax = %d\n' % int(factor*curr_value)) 
                    elif curr_recid == 13: # 
                        tmpbuf.write('flow = %d\n' % int(factor*curr_value*1000)) # flow converted from m3/h to dm3/h 
                    elif curr_recid == 14: # 
                        tmpbuf.write('flowMax = %d\n' % int(factor*curr_value*1000)) # maxflow converted from m3/h to dm3/h 
                    elif curr_recid == 15: # 
                        tmpbuf.write('errorFlags = \'%s\'\n' % str(curr_value)) 
                    elif curr_recid == 16: # 
                        tmpbuf.write('timePoint = \'%s\'\n' % str(curr_value)) 
                    elif curr_recid == 17: # 
                        tmpbuf.write('targetHeatingEnergy = %d\n' % int(factor*curr_value)) 
                    elif curr_recid == 18: # 
                        tmpbuf.write('targetCoolingEnergy = %d\n' % int(factor*curr_value)) 
                    elif curr_recid == 19: # 
                        tmpbuf.write('targetEnergyM3T1 = %d\n' % int(curr_value)) 
                    elif curr_recid == 20: # 
                        tmpbuf.write('targetEnergyM3T2 = %d\n' % int(curr_value)) 
                    elif curr_recid == 21: # 
                        tmpbuf.write('targetVolume = %d\n' % int(factor*curr_value*1000)) # volume converted from m3 to dm3
                    elif curr_recid == 22: # 
                        tmpbuf.write('targetPowerMax = %d\n' % int(factor*curr_value)) 
                    elif curr_recid == 23: # 
                        tmpbuf.write('targetFlowMax = %d\n' % int(factor*curr_value)) 
                    elif curr_recid == 24: # 
                        tmpbuf.write('targetTimepoint = \'%s\'\n' % str(curr_value)) 
                    elif curr_recid == 25: # 
                        _log.debug('Skipping record id 25')
                        continue
                    elif curr_recid == 26: # 
                        _log.debug('Skipping record id 26')
                        continue
                    elif curr_recid == 27: # 
                        _log.debug('Skipping record id 27')
                        continue
                    elif curr_recid == 28: # 
                        _log.debug('Skipping record id 28')
                        continue
                    elif curr_recid == 29: # 
                        _log.debug('Skipping record id 29')
                        continue
                    else:
                        continue
                    tmpbuf.write('WHERE id = \'%s\' AND recordTimestamp = \'%s\';\n' % (curr_id,curr_ts))
                    sqlbuf.write(tmpbuf.getvalue())
                _log.debug(sqlbuf.getvalue())
                result = []
                try:
                    result = yield DBHelperSQLITE.dbExecute(_dbHelper,sqlbuf.getvalue(),parseresp=None,script=True,returnrowcount=True)
                except Exception as e:
                    _log.warning('Error during database operation : %s' % str(e))
                    pass
        _log.info("Aggregation done")
        return


class BackgroundTaskHandler(web.RequestHandler):

    @gen.coroutine
    def get(self):
        global dbAggregation_running

        turn = self.get_argument('turn',None)
        if (turn == 'on'):
            dbAggregation_running = True
            _log.info('Turn data aggregation \'on\', AggregationRunning : %s' % dbAggregation_running)
        elif (turn == 'off'):
            dbAggregation_running = False
            _log.info('Turn data aggregation \'off\', AggregationRunning : %s' % dbAggregation_running)
        else:
            # do nothing
            _log.info('Request for data aggregation current state, AggregationRunning : %s' % dbAggregation_running)
    
        loglvl = self.get_argument('loglevel',None)
        if loglvl and len(loglvl) > 0:
            lvl = logging.getLevelName(loglvl)
            if lvl:
                try:
                    _log.info('Setting log-level to  : %s' % logging.getLevelName(lvl))
                    _log.setLevel(lvl)
                except Exception:
                    _log.warning('Incorrect log level \'%s\', loglevel unchanged' % lvl)
        else:
            # do nothing
            _log.info('Request for data aggregation current state, AggregationRunning : %s' % dbAggregation_running)

        power = self.get_argument('power',None)
        if power == 'poll':
            _log.info('Calling PowerPoller')
            try:
                res = yield PowerPoller.getPowerData()
            except Exception as e:
                pass

        self.write('<html><body><div>BG LOOP RUNNING : %s</div><div>LOG_LEVEL : %s</div></body></html>' % (dbAggregation_running,logging.getLevelName(_log.getEffectiveLevel())))
        self.finish()

# Handler for main page
class MainHandler(web.RequestHandler):
    def get(self):
        # Returns rendered template string to the browser request
        _log.info("In main handler")
        self.write('OK')

#
class DataRequestHandler(web.RequestHandler):
    @gen.coroutine
    def get(self):
        _log.info("New data request")
        q = self.get_argument('q',None)
        resp = ''
        if (q == 'mbus') or (q == 'power'):
            todaystartutc = datetime.utcfromtimestamp(divmod(time.time(),86400)[0]*86400).strftime(df)
            now = datetime.utcnow().strftime(df)
            mints = self.get_argument('mints',None) 
            maxts = self.get_argument('maxts',None)
            _log.info('mints = %s maxts = %s' % (mints,maxts))

            sql='SELECT * from aMbusMC302Record WHERE recordTimestamp ' if q == 'mbus' else 'SELECT * from aPower WHERE ts '
            if mints and maxts:
                sql += 'between \'%s\' and \'%s\'' % (mints,maxts)
            elif mints:
                sql += '> \'%s\'' % (mints)
            elif maxts:
                sql += '< \'%s\'' % (maxts)
            sql += ';'
            _log.debug('DB SQL:\n%s' % sql)
            
            try:
                resp = yield DBHelperSQLITE.dbExecute(_dbHelper,sql,parseresp='CSV',script=False,returnrowcount=False)
            except Exception as e:
                _log.warning('Error during database operation : %s' % str(e))
                pass

        else:
            _log.warning('Bad request %s' % self.request.uri)
        self.write(resp)
        self.finish()

# Assign handler to the server root  (127.0.0.1:PORT/)
application = web.Application([
    (r"/", MainHandler),
    (r'/static/(.*)', web.StaticFileHandler, {'path': static_path}),
    (r"/bg",BackgroundTaskHandler),
    (r"/data",DataRequestHandler)
])
PORT=8888
if __name__ == "__main__":
    import logging
    logFormat = '%(asctime)s:%(levelname)s:%(funcName)s:%(message)s'
    logging.basicConfig(format=logFormat)
    
#    options.options['log_file_prefix'].set('/opt/logs/my_app.log')
    #options.parse_command_line() # read the command-line to get log_file_prefix from there
    _log = logging.getLogger("tornado.application")
    _log.critical('\n--------------------------------------------------------------------------\nApplication starting')
    _log.setLevel(LOG_LEVEL)   # 50 critical, 40 error, 30 warning, 20 info, 10 debug
    _log.critical('Current log-level : %s',logging.getLevelName(_log.getEffectiveLevel()))
    #_setupsql() # Prepare sqls
    # Setup the server
    application.listen(PORT)
    _log.info('Listening on port %d' % PORT)
    _dbHelper = DBHelperSQLITE()
    _scheduler = TornadoScheduler()
    _scheduler.start()

    ioloop = IOLoop.instance()
    _bgtask = TaskRunner(ioloop)

    TaskRunner.bgsetup(_bgtask)

    _log.info('Background task is set up, starting ioloop')
    ioloop.start()