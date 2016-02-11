CREATE TABLE aPower (
  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  `eventNanoTs` bigint(20) NOT NULL,
  `ts` varchar(40) NOT NULL,
  `circuit9_cumul` int(11) DEFAULT NULL,
  `circuit10_cumul` int(11) DEFAULT NULL,
  `circuit11_cumul` int(11) DEFAULT NULL,
  `circuit12_cumul` int(11) DEFAULT NULL,
  `circuit13_cumul` int(11) DEFAULT NULL,
  `circuit14_cumul` int(11) DEFAULT NULL,
  `circuit15_cumul` int(11) DEFAULT NULL,
  `circuit16_cumul` int(11) DEFAULT NULL,
  `circuit9_curr` int(11) DEFAULT NULL,
  `circuit10_curr` int(11) DEFAULT NULL,
  `circuit11_curr` int(11) DEFAULT NULL,
  `circuit12_curr` int(11) DEFAULT NULL,
  `circuit13_curr` int(11) DEFAULT NULL,
  `circuit14_curr` int(11) DEFAULT NULL,
  `circuit15_curr` int(11) DEFAULT NULL,
  `circuit16_curr` int(11) DEFAULT NULL,
  UNIQUE(ts)
);

CREATE TABLE aPowerStats (
    id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    groupNanoTs BIGINT(20) NOT NULL,
    c1_use INT(11),
    c1_cumul INT(11),
    c1_peak INT(11),
    c2_use INT(11),
    c2_cumul INT(11),
    c2_peak INT(11),
    c3_use INT(11),
    c3_cumul INT(11),
    c3_peak INT(11),
    c4_use INT(11),
    c4_cumul INT(11),
    c4_peak INT(11),
    c5_use INT(11),
    c5_cumul INT(11),
    c5_peak INT(11),
    c6_use INT(11),
    c6_cumul INT(11),
    c6_peak INT(11),
    c7_use INT(11),
    c7_cumul INT(11),
    c7_peak INT(11),
    c8_use INT(11),
    c8_cumul INT(11),
    c8_peak INT(11),
    all_use INT(11),
    all_cumul INT(11),
    all_peak INT(11),
    measures_count INT(11),
    deltaNanoTs BIGINT(20), /* timestamp of previous measurement used for delta */
    groupTimestamp VARCHAR(40), /* %Y%m%dT%H%M%S */
    rowCreatedTimestamp DATETIME(6) DEFAULT CURRENT_TIMESTAMP(6),  /* row inserted at timestamp  */
    rowUpdatedTimestamp DATETIME(6) DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP(6), /* row updated at timestamp  */
    UNIQUE (groupNanoTs)
);

UPDATE aPower SET eventNanoTs = CAST((UNIX_TIMESTAMP(STR_TO_DATE(ts,'%Y-%m-%d %k:%i:%s.%f'))*1000000000) AS SIGNED);



SELECT 
  t2.gts,t2.mts,t2.c4_peak,t2.c4_cumul,t2.cnt
  , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*60e9) , @prev_ts, -1) AS ts_prev
  , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*60e9) && @prev_c12_cumul > -1, @prev_c12_cumul, -1 ) AS c4_prev
  , @prev_ts := t2.gts  
  , @prev_c12_cumul := t2.c4_cumul
  FROM 
    (SELECT (t1.eventNanoTs DIV 60e9)*60e9+60e9 as gts, 
      max(t1.ts) as mts, 
      MAX(t1.circuit12_curr) AS c4_peak, 
      MAX(t1.circuit12_cumul) AS c4_cumul, 
      count(t1.id) AS cnt 
      FROM
      aPower t1
      WHERE eventNanoTs >= @ts 
      AND eventNanoTs < (UNIX_TIMESTAMP() DIV 60)*60e9 /* skip currently running minute */
      GROUP BY gts
    ) t2,  
    (select @prev_ts := IFNULL((select (t3.eventNanoTs DIV 60e9)*60e9+60e9 FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1)
            , @prev_c12_cumul := IFNULL((select t3.circuit12_cumul FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1)
    FROM (select @ts := 1453732800000000000) TS ) SQLVars

      



INSERT IGNORE INTO aPowerStats (groupNanoTs,measures_count,deltaNanoTs,groupTimestamp
  ,c1_peak,c1_cumul,c1_use
  ,c2_peak,c2_cumul,c2_use 
  ,c3_peak,c3_cumul,c3_use 
  ,c4_peak,c4_cumul,c4_use 
  ,c5_peak,c5_cumul,c5_use 
  ,c6_peak,c6_cumul,c6_use 
  ,c7_peak,c7_cumul,c7_use 
  ,c8_peak,c8_cumul,c8_use 
  ) 
  SELECT CAST(aa.gts AS SIGNED) as groupNanoTs,aa.cnt,CAST(aa.ts_prev AS SIGNED) as deltaNanoTs, FROM_UNIXTIME(aa.gts/1e9) as groupTimestamp,
    aa.c1_peak,aa.c1_cumul,if(aa.ts_prev < 0,NULL,aa.c1_cumul-aa.c1_prev) as c1_use, 
    aa.c2_peak,aa.c2_cumul,if(aa.ts_prev < 0,NULL,aa.c2_cumul-aa.c2_prev) as c2_use, 
    aa.c3_peak,aa.c3_cumul,if(aa.ts_prev < 0,NULL,aa.c3_cumul-aa.c3_prev) as c3_use, 
    aa.c4_peak,aa.c4_cumul,if(aa.ts_prev < 0,NULL,aa.c4_cumul-aa.c4_prev) as c4_use, 
    aa.c5_peak,aa.c5_cumul,if(aa.ts_prev < 0,NULL,aa.c5_cumul-aa.c5_prev) as c5_use, 
    aa.c6_peak,aa.c6_cumul,if(aa.ts_prev < 0,NULL,aa.c6_cumul-aa.c6_prev) as c6_use, 
    aa.c7_peak,aa.c7_cumul,if(aa.ts_prev < 0,NULL,aa.c7_cumul-aa.c7_prev) as c7_use, 
    aa.c8_peak,aa.c8_cumul,if(aa.ts_prev < 0,NULL,aa.c8_cumul-aa.c8_prev) as c8_use 
    FROM ( 
    SELECT t2.gts,t2.cnt
      , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) , @prev_ts, -1) AS ts_prev
      ,t2.c1_peak, t2.c1_cumul 
      ,t2.c2_peak, t2.c2_cumul 
      ,t2.c3_peak, t2.c3_cumul 
      ,t2.c4_peak, t2.c4_cumul 
      ,t2.c5_peak, t2.c5_cumul 
      ,t2.c6_peak, t2.c6_cumul 
      ,t2.c7_peak, t2.c7_cumul 
      ,t2.c8_peak, t2.c8_cumul 
      , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) && @prev_c9_cumul > -1, @prev_c9_cumul, -1 ) AS c1_prev
      , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) && @prev_c10_cumul > -1, @prev_c10_cumul, -1 ) AS c2_prev
      , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) && @prev_c11_cumul > -1, @prev_c11_cumul, -1 ) AS c3_prev
      , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) && @prev_c12_cumul > -1, @prev_c12_cumul, -1 ) AS c4_prev
      , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) && @prev_c13_cumul > -1, @prev_c13_cumul, -1 ) AS c5_prev
      , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) && @prev_c14_cumul > -1, @prev_c14_cumul, -1 ) AS c6_prev
      , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) && @prev_c15_cumul > -1, @prev_c15_cumul, -1 ) AS c7_prev
      , if( @prev_ts > -1 && (t2.gts - @prev_ts) < (2*@grpsec) && @prev_c16_cumul > -1, @prev_c16_cumul, -1 ) AS c8_prev
      , @prev_ts := t2.gts  
      , @prev_c9_cumul := t2.c1_cumul
      , @prev_c10_cumul := t2.c2_cumul
      , @prev_c11_cumul := t2.c3_cumul
      , @prev_c12_cumul := t2.c4_cumul
      , @prev_c13_cumul := t2.c5_cumul
      , @prev_c14_cumul := t2.c6_cumul
      , @prev_c15_cumul := t2.c7_cumul
      , @prev_c16_cumul := t2.c8_cumul
      FROM 
        (SELECT (t1.eventNanoTs DIV @grpsec)*@grpsec+@grpsec as gts, count(t1.id) AS cnt 
          ,MAX(t1.circuit9_curr) AS c1_peak, MAX(t1.circuit9_cumul) AS c1_cumul 
          ,MAX(t1.circuit10_curr) AS c2_peak, MAX(t1.circuit10_cumul) AS c2_cumul 
          ,MAX(t1.circuit11_curr) AS c3_peak, MAX(t1.circuit11_cumul) AS c3_cumul 
          ,MAX(t1.circuit12_curr) AS c4_peak, MAX(t1.circuit12_cumul) AS c4_cumul 
          ,MAX(t1.circuit13_curr) AS c5_peak, MAX(t1.circuit13_cumul) AS c5_cumul 
          ,MAX(t1.circuit14_curr) AS c6_peak, MAX(t1.circuit14_cumul) AS c6_cumul 
          ,MAX(t1.circuit15_curr) AS c7_peak, MAX(t1.circuit15_cumul) AS c7_cumul 
          ,MAX(t1.circuit16_curr) AS c8_peak, MAX(t1.circuit16_cumul) AS c8_cumul 
          FROM
          aPower t1
          WHERE eventNanoTs >= @ts
          AND eventNanoTs < @maxts
          GROUP BY gts
        ) t2,  
        (select @prev_ts := IFNULL((select (t3.eventNanoTs DIV @grpsec)*@grpsec+@grpsec FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1) 
          ,@prev_c9_cumul := IFNULL((select t3.circuit9_cumul FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1)
          ,@prev_c10_cumul := IFNULL((select t3.circuit10_cumul FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1)
          ,@prev_c11_cumul := IFNULL((select t3.circuit11_cumul FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1)
          ,@prev_c12_cumul := IFNULL((select t3.circuit12_cumul FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1)
          ,@prev_c13_cumul := IFNULL((select t3.circuit13_cumul FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1)
          ,@prev_c14_cumul := IFNULL((select t3.circuit14_cumul FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1)
          ,@prev_c15_cumul := IFNULL((select t3.circuit15_cumul FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1)
          ,@prev_c16_cumul := IFNULL((select t3.circuit16_cumul FROM aPower t3 WHERE eventNanoTs < @ts ORDER BY id DESC LIMIT 1),-1)
        FROM (select @ts := 0, @grpsec := 60e9, @maxts := (UNIX_TIMESTAMP() DIV 60)*60e9) TS ) SQLVars
      ) aa
;






