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

      









