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
    c1_producer BOOLEAN DEFAULT 0, /* by default the circuit is considered a consumer, ie producer = False or 0 */
    c1_use INT(11),
    c1_cumul INT(11),
    c1_peak INT(11),
    c2_producer BOOLEAN DEFAULT 0,
    c2_use INT(11),
    c2_cumul INT(11),
    c2_peak INT(11),
    c3_producer BOOLEAN DEFAULT 0,
    c3_use INT(11),
    c3_cumul INT(11),
    c3_peak INT(11),
    c4_producer BOOLEAN DEFAULT 0,
    c4_use INT(11),
    c4_cumul INT(11),
    c4_peak INT(11),
    c5_producer BOOLEAN DEFAULT 0,
    c5_use INT(11),
    c5_cumul INT(11),
    c5_peak INT(11),
    c6_producer BOOLEAN DEFAULT 0,
    c6_use INT(11),
    c6_cumul INT(11),
    c6_peak INT(11),
    c7_producer BOOLEAN DEFAULT 0,
    c7_use INT(11),
    c7_cumul INT(11),
    c7_peak INT(11),
    c8_producer BOOLEAN DEFAULT 0,
    c8_use INT(11),
    c8_cumul INT(11),
    c8_peak INT(11),
    measures_count INT(11),
    deltaNanoTs BIGINT(20), /* timestamp of previous measurement used for delta */
    groupTimestamp VARCHAR(40), /* %Y%m%dT%H%M%S */
    rowCreatedTimestamp DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),  /* row inserted at timestamp  */
    rowUpdatedTimestamp DATETIME(6) DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP(6), /* row updated at timestamp  */
    UNIQUE (groupNanoTs)
);


SELECT 
  t2.gts,t2.gts2,t2.mts,t2.c1_peak,t2.c1_cumul,t2.cnt
  , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) , @prev_ts, -1) AS ts_prev
  , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) && @prev_c1_cumul > -1, @prev_c1_cumul, -1 ) AS c1_prev
  , @prev_ts := t2.gts  
  , @prev_c1_cumul := t2.c1_cumul
  FROM 
    (SELECT (t1.recordNanoTs DIV @grpsec)*@grpsec+@grpsec as gts, 
      FROM_UNIXTIME(((t1.recordNanoTs DIV @grpsec)*@grpsec+@grpsec)/1e9) as gts2,
      max(t1.recordTimestamp) as mts, 
      MAX(t1.power) AS c1_peak, 
      MAX(t1.heatingEnergy) AS c1_cumul, 
      count(t1.rowId) AS cnt 
      FROM
      aMbusMC302Record t1
      WHERE id = '67285016' AND
      AND recordNanoTs >= @ts 
      AND recordNanoTs < @maxts /* skip currently running minute */
      GROUP BY gts
    ) t2,  
    (select @prev_ts := IFNULL((select (t.recordNanoTs DIV @grpsec)*@grpsec+@grpsec FROM aMbusMC302Record t WHERE id = '67285016' AND recordNanoTs < @ts ORDER BY rowId DESC LIMIT 1),-1)
            , @prev_c1_cumul := IFNULL((select t.heatingEnergy FROM aMbusMC302Record t WHERE id = '67285016' AND recordNanoTs < @ts ORDER BY rowId DESC LIMIT 1),-1)
    FROM (select @ts := 0, @grpsec := 60e9, @maxts := (UNIX_TIMESTAMP() DIV 60)*60e9) TS ) SQLVars
LIMIT 10;

    FROM (select @ts := 1453732800000000000, @grpsec := 60e9, @maxts := (UNIX_TIMESTAMP() DIV 60)*60e9) TS ) SQLVars
