DROP TABLE IF EXISTS aPowerStats;

CREATE TABLE aPowerStats (
    id INTEGER PRIMARY KEY NOT NULL,
    ts TEXT NOT NULL, /* %Y%m%dT%H%M%S */
    c1_use INTEGER,
    c1_cumul INTEGER,
    c1_peak INTEGER,
    c2_use INTEGER,
    c2_cumul INTEGER,
    c2_peak INTEGER,
    c3_use INTEGER,
    c3_cumul INTEGER,
    c3_peak INTEGER,
    c4_use INTEGER,
    c4_cumul INTEGER,
    c4_peak INTEGER,
    c5_use INTEGER,
    c5_cumul INTEGER,
    c5_peak INTEGER,
    c6_use INTEGER,
    c6_cumul INTEGER,
    c6_peak INTEGER,
    c7_use INTEGER,
    c7_cumul INTEGER,
    c7_peak INTEGER,
    c8_use INTEGER,
    c8_cumul INTEGER,
    c8_peak INTEGER,
    all_use INTEGER,
    all_cumul INTEGER,
    all_peak INTEGER,
    measures_count INTEGER,
    delta_ts TEXT, /* timestamp of previous measurement used for delta */
    update_ts TIMESTAMP DATETIME DEFAULT(STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW'))  /* row inserted at timestamp  */
);

INSERT INTO aPowerStats (ts,measures_count
    ,c1_peak,c1_cumul
    ,c2_peak,c2_cumul
    ,c3_peak,c3_cumul
    ,c4_peak,c4_cumul
    ,c5_peak,c5_cumul
    ,c6_peak,c6_cumul
    ,c7_peak,c7_cumul
    ,c8_peak,c8_cumul
    ,all_peak,all_cumul) 
SELECT groupedts
    ,count(*) AS measures_count
    ,max(circuit9_curr) AS c1_peak, max(circuit9_cumul) AS c1_cumul 
    ,max(circuit10_curr) AS c2_peak, max(circuit10_cumul) AS c2_cumul 
    ,max(circuit11_curr) AS c3_peak, max(circuit11_cumul) AS c3_cumul 
    ,max(circuit12_curr) AS c4_peak, max(circuit12_cumul) AS c4_cumul 
    ,max(circuit13_curr) AS c5_peak, max(circuit13_cumul) AS c5_cumul 
    ,max(circuit14_curr) AS c6_peak, max(circuit14_cumul) AS c6_cumul 
    ,max(circuit15_curr) AS c7_peak, max(circuit15_cumul) AS c7_cumul 
    ,max(circuit16_curr) AS c8_peak, max(circuit16_cumul) AS c8_cumul 
    ,max(all_peak) AS all_peak
    ,max(all_cumul) AS all_cumul 
    FROM ( 
        SELECT
            ts, 
            datetime((strftime('%s', ts) / 86400) * 86400 + 86400, 'unixepoch') AS groupedts,
            circuit9_curr,circuit10_curr,circuit11_curr,circuit12_curr,circuit13_curr,circuit14_curr,circuit15_curr,circuit16_curr,
            circuit9_cumul,circuit10_cumul,circuit11_cumul,circuit12_cumul,circuit13_cumul,circuit14_cumul,circuit15_cumul,circuit16_cumul,
            (ifnull(circuit9_curr,0)+ifnull(circuit10_curr,0)+ifnull(circuit11_curr,0)+ifnull(circuit12_curr,0)+ifnull(circuit13_curr,0)+ifnull(circuit14_curr,0)+ifnull(circuit15_curr,0)+ifnull(circuit16_curr,0)) AS all_peak,
            (ifnull(circuit9_cumul,0)+ifnull(circuit10_cumul,0)+ifnull(circuit11_cumul,0)+ifnull(circuit12_cumul,0)+ifnull(circuit13_cumul,0)+ifnull(circuit14_cumul,0)+ifnull(circuit15_cumul,0)+ifnull(circuit16_cumul,0)) AS all_cumul
            FROM aPower
            WHERE groupedts > ifnull((SELECT max(ts) FROM aPowerStats),0) /* only process entries for group newer than last group added to stats-table */
                AND ts < datetime((strftime('%s', 'NOW') / 86400) * 86400, 'unixepoch') /* leave most recent entries (= now() - at most 5min) since the clock is ticking and all data for group isn't available yet */
        )
    GROUP BY groupedts 
    ORDER BY groupedts;

WITH input AS (
  SELECT 
    t1.id AS id, 
    t1.ts AS ts1, 
    t2.ts AS ts2,
    t1.c1_cumul AS t1_c1, 
    t2.c1_cumul AS t2_c1,
    t1.c1_cumul-t2.c1_cumul AS delta_c1, 
    t1.c2_cumul AS t1_c2, 
    t2.c2_cumul AS t2_c2,
    t1.c2_cumul-t2.c2_cumul AS delta_c2, 
    t1.c3_cumul AS t1_c3, 
    t2.c3_cumul AS t2_c3,
    t1.c3_cumul-t2.c3_cumul AS delta_c3, 
    t1.c4_cumul AS t1_c4, 
    t2.c4_cumul AS t2_c4,
    t1.c4_cumul-t2.c4_cumul AS delta_c4, 
    t1.c5_cumul AS t1_c5, 
    t2.c5_cumul AS t2_c5,
    t1.c5_cumul-t2.c5_cumul AS delta_c5, 
    t1.c6_cumul AS t1_c6, 
    t2.c6_cumul AS t2_c6,
    t1.c6_cumul-t2.c6_cumul AS delta_c6, 
    t1.c7_cumul AS t1_c7, 
    t2.c7_cumul AS t2_c7,
    t1.c7_cumul-t2.c7_cumul AS delta_c7, 
    t1.c8_cumul AS t1_c8, 
    t2.c8_cumul AS t2_c8,
    t1.c8_cumul-t2.c8_cumul AS delta_c8, 
    t1.all_cumul AS t1_all, 
    t2.all_cumul AS t2_all,
    t1.all_cumul-t2.all_cumul AS delta_all 
  FROM aPowerStats t1 
  LEFT OUTER JOIN aPowerStats t2 
    ON t2.id = coalesce(
      (SELECT t3.id FROM aPowerStats t3
        WHERE t3.id < t1.id 
        ORDER BY id DESC 
        LIMIT 1), 
      t1.id)
  )
UPDATE aPowerStats SET 
    c1_use = (SELECT ifnull(delta_c1,0) FROM input i WHERE aPowerStats.id = i.id),
    c2_use = (SELECT ifnull(delta_c2,0) FROM input i WHERE aPowerStats.id = i.id),
    c3_use = (SELECT ifnull(delta_c3,0) FROM input i WHERE aPowerStats.id = i.id),
    c4_use = (SELECT ifnull(delta_c4,0) FROM input i WHERE aPowerStats.id = i.id),
    c5_use = (SELECT ifnull(delta_c5,0) FROM input i WHERE aPowerStats.id = i.id),
    c6_use = (SELECT ifnull(delta_c6,0) FROM input i WHERE aPowerStats.id = i.id),
    c7_use = (SELECT ifnull(delta_c7,0) FROM input i WHERE aPowerStats.id = i.id),
    c8_use = (SELECT ifnull(delta_c8,0) FROM input i WHERE aPowerStats.id = i.id),
    all_use = (SELECT ifnull(delta_all,0) FROM input i WHERE aPowerStats.id = i.id),
    delta_ts = (SELECT ts2 FROM input i WHERE aPowerStats.id = i.id);
