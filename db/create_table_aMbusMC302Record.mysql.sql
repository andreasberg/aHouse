CREATE TABLE IF NOT EXISTS aMbusMC302Record(
    rowId INT(11) PRIMARY KEY NOT NULL,
    recordNanoTs BIGINT(20) NOT NULL,
    id VARCHAR(40) NOT NULL,            /* Mbus slave (SlaveInformation) id*/
    accessNumber INTEGER NOT NULL,  /* Slave MBus response sequence number */
    heatingEnergy INTEGER,      /* Heating energy (cumulative, non-resettable) converted to Wh from     1|Instantaneous value|0|Energy;100;Wh */
    coolingEnergy INTEGER,      /* Cooling energy (cumulative, non-resettable) converted to Wh from     2|Instantaneous value|0|Energy;100;Wh */
    energyM3T1 INTEGER,     /* Energy mˆ3 * T1 (cumulative Volume * Temperature) from       3|Instantaneous value|0|Manufacturer specific */
    energyM3T2 INTEGER,     /* Energy mˆ3 * T2 (cumulative Volume * Temperature) from       4|Instantaneous value|0|Manufacturer specific   */
    volume INTEGER,         /* Current volume (cumulative converted to dm3) from            5|Instantaneous value|0|Volume;m;m^3 */
    hourCounter INTEGER,        /* Hour counter (non-resettable) from                   6|Instantaneous value|0|On time (hours) */
    errorHourCounter INTEGER,   /* Error hour counter (cumul hours in error, no resettable) from    7|Value during error state|0|On time (hours) */
    temp1 REAL,         /* Current (Flow) Temperature T1 (converted to deg.Celsius) from    8|Instantaneous value|0|Flow temperature;1e-2;deg C */
    temp2 REAL,         /* Current (Return) Temperature T2 (converted to deg.Celsius) from  9|Instantaneous value|0|Return temperature;1e-2;deg C */
    deltaT1T2 REAL,         /* Temperature difference T1-T2 (converted to  deg.Celsius) from    10|Instantaneous value|0|Temperature Difference;1e-2;deg C */
    power INTEGER,          /* Current power (converted to W) from                  11|Instantaneous value|0|Power;100;W */
    powerMax INTEGER,       /* Maximum power since XX (converted to Watt) from          12|Maximum value|0|Power;100;W */
    flow INTEGER,           /* Current water flow (converted to dm3/h) from             13|Instantaneous value|0|Volume flow;m;m^3/h  */
    flowMax INTEGER,        /* Maximum water flow since XX (converted to dm3/h)         14|Maximum value|0|Volume flow;m;m^3/h */
    errorFlags VARCHAR(40),     /* Error flags from                         15|Instantaneous value|0|Error flags */
    timePoint VARCHAR(40),          /* Date+Time from                           16|Instantaneous value|0|Time Point (time & date) */
    targetHeatingEnergy INTEGER,/* Heating energy since targetTimepoint (in Wh) from            17|Instantaneous value|1|Energy;100;Wh */
    targetCoolingEnergy INTEGER,    /* Cooling energy since targetTimepoint (in Wh) from            18|Instantaneous value|1|Energy;100;Wh */
    targetEnergyM3T1 INTEGER,   /* Energy mˆ3 * T1 since targetTimepoint from               19|Instantaneous value|1|Manufacturer specific */
    targetEnergyM3T2 INTEGER,   /* Energy mˆ3 * T2 since targetTimepoint from               20|Instantaneous value|1|Manufacturer specific */
    targetVolume INTEGER,       /* Volume since targetTimepoint (in dm3) from               21|Instantaneous value|1|Volume;m;m^3 */
    targetPowerMax INTEGER,     /* Max power since targetTimepoint (in Watt) from           22|Maximum value|1|Power;100;W  */
    targetFlowMax INTEGER,      /* Max flow since targetTimepoint (in dm3/h) from           23|Maximum value|1|Volume flow;m;m^3/h  */
    targetTimepoint VARCHAR(40),        /* Target time point (date) from                    24|Instantaneous value|1|Time Point (date) */
    address INTEGER,    /* Mbus primary address*/
    recordTimestampRaw VARCHAR(40),     /* same for all data records in single Mbus response*/
    recordTimestamp VARCHAR(40),        /* parsed value of raw */
    rowCreatedTimestamp VARCHAR(40) NOT NULL,
    UNIQUE (accessNumber,id,recordTimestamp)
);






CREATE TABLE aWaterEnergyStats (
    id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    groupNanoTs BIGINT(20) NOT NULL,
    c1_indirect BOOLEAN DEFAULT 0, /* by default the circuit is considered a consumer, ie producer = False or 0 */
    c1_delta INT(11),
    c1_cumul INT(11),
    c1_peak INT(11),
    c2_indirect BOOLEAN DEFAULT 0,
    c2_delta INT(11),
    c2_cumul INT(11),
    c2_peak INT(11),
    c3_indirect BOOLEAN DEFAULT 0,
    c3_delta INT(11),
    c3_cumul INT(11),
    c3_peak INT(11),
    c4_indirect BOOLEAN DEFAULT 0,
    c4_delta INT(11),
    c4_cumul INT(11),
    c4_peak INT(11),
    c5_indirect BOOLEAN DEFAULT 0,
    c5_delta INT(11),
    c5_cumul INT(11),
    c5_peak INT(11),
    c6_indirect BOOLEAN DEFAULT 0,
    c6_delta INT(11),
    c6_cumul INT(11),
    c6_peak INT(11),
    c7_indirect BOOLEAN DEFAULT 0,
    c7_delta INT(11),
    c7_cumul INT(11),
    c7_peak INT(11),
    c8_indirect BOOLEAN DEFAULT 0,
    c8_delta INT(11),
    c8_cumul INT(11),
    c8_peak INT(11),
    measures_count INT(11),
    deltaNanoTs BIGINT(20), /* timestamp of previous measurement used for delta */
    groupTimestamp VARCHAR(40), /* %Y%m%dT%H%M%S */
    rowCreatedTimestamp DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),  /* row inserted at timestamp  */
    rowUpdatedTimestamp DATETIME(6) DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP(6), /* row updated at timestamp  */
    UNIQUE (groupNanoTs)
);

INSERT INTO aWaterEnergyStats (groupNanoTs,measures_count,deltaNanoTs,groupTimestamp
  ,c1_peak,c1_cumul,c1_delta
  ,c2_peak,c2_cumul,c2_delta
  ,c3_peak,c3_cumul,c3_delta
  ) 
  SELECT CAST(aa.gts AS SIGNED) as groupNanoTs,aa.cnt,aa.ts_prev as deltaNanoTs, FROM_UNIXTIME(aa.gts/1e9) as groupTimestamp,
    aa.c1_peak,aa.c1_cumul,if(aa.ts_prev < 0,NULL,aa.c1_cumul-aa.c1_prev) as c1_delta,  
    aa.c2_peak,aa.c2_cumul,if(aa.ts_prev < 0,NULL,aa.c2_cumul-aa.c2_prev) as c2_delta,  
    aa.c3_peak,aa.c3_cumul,if(aa.ts_prev < 0,NULL,aa.c3_cumul-aa.c3_prev) as c3_delta 
    FROM (
        SELECT t1.gts,t1.cnt
          , if( @prev_ts > -1 && (t1.gts - @prev_ts) < (2*@grpsec) , @prev_ts, -1) AS ts_prev
          , t1.c1_peak,t1.c1_cumul
          , if( @prev_ts > -1 && (t1.gts - @prev_ts) < (2*@grpsec) && @prev_c1_cumul > -1, @prev_c1_cumul, -1 ) AS c1_prev
          , t2.c2_peak,t2.c2_cumul
          , if( @prev_ts > -1 && (t1.gts - @prev_ts) < (2*@grpsec) && @prev_c2_cumul > -1, @prev_c2_cumul, -1 ) AS c2_prev
          , t3.c3_peak,t3.c3_cumul
          , if( @prev_ts > -1 && (t1.gts - @prev_ts) < (2*@grpsec) && @prev_c3_cumul > -1, @prev_c3_cumul, -1 ) AS c3_prev
          , @prev_ts := t1.gts  
          , @prev_c1_cumul := t1.c1_cumul
          , @prev_c2_cumul := t2.c2_cumul
          , @prev_c3_cumul := t3.c3_cumul
          FROM 
            (SELECT (recordNanoTs DIV @grpsec)*@grpsec+@grpsec as gts, 
                  MAX(power) AS c1_peak, 
                  MAX(heatingEnergy) AS c1_cumul, 
                  count(rowId) AS cnt 
                  FROM aMbusMC302Record
                  WHERE id = '67285016'
                  AND recordNanoTs >= @ts 
                  AND recordNanoTs < @maxts /* skip currently running minute */
                  GROUP BY gts
                ) t1 LEFT JOIN  
                ((SELECT (recordNanoTs DIV @grpsec)*@grpsec+@grpsec as gts, 
                  MAX(power) AS c2_peak, 
                  MAX(heatingEnergy) AS c2_cumul 
                  FROM aMbusMC302Record
                  WHERE id = '67285015'
                  AND recordNanoTs >= @ts 
                  AND recordNanoTs < @maxts /* skip currently running minute */
                  GROUP BY gts
                ) t2,
                (SELECT (recordNanoTs DIV @grpsec)*@grpsec+@grpsec as gts, 
                  MAX(power) AS c3_peak, 
                  MAX(heatingEnergy) AS c3_cumul 
                  FROM aMbusMC302Record
                  WHERE id = '67280331'
                  AND recordNanoTs >= @ts 
                  AND recordNanoTs < @maxts /* skip currently running minute */
                  GROUP BY gts
                ) t3) ON (t2.gts=t1.gts AND t3.gts=t1.gts) ,  
            (select 
                @prev_ts := IFNULL((select (t.recordNanoTs DIV @grpsec)*@grpsec+@grpsec FROM aMbusMC302Record t WHERE id = '67285016' AND recordNanoTs < @ts ORDER BY recordNanoTs DESC LIMIT 1),-1)
                ,@prev_c1_cumul := IFNULL((select t.heatingEnergy FROM aMbusMC302Record t WHERE id = '67285016' AND recordNanoTs < @ts ORDER BY recordNanoTs DESC LIMIT 1),-1)
                ,@prev_c2_cumul := IFNULL((select t.heatingEnergy FROM aMbusMC302Record t WHERE id = '67285015' AND recordNanoTs < @ts ORDER BY recordNanoTs DESC LIMIT 1),-1)
                ,@prev_c3_cumul := IFNULL((select t.heatingEnergy FROM aMbusMC302Record t WHERE id = '67280331' AND recordNanoTs < @ts ORDER BY recordNanoTs DESC LIMIT 1),-1)
                FROM (select @ts := 0, @grpsec := 60e9, @maxts := (UNIX_TIMESTAMP() DIV 60)*60e9) TS 
                ) SQLVars
    ) aa
    ;

LIMIT 10;

    FROM (select @ts := 1453732800000000000, @grpsec := 60e9, @maxts := (UNIX_TIMESTAMP() DIV 60)*60e9) TS ) SQLVars
