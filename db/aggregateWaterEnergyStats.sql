INSERT INTO aWaterEnergyStats (groupNanoTs,measures_count,deltaNanoTs,groupTimestamp
  ,c1_peak,c1_cumul,c1_delta
  ,c2_peak,c2_cumul,c2_delta
  ,c3_peak,c3_cumul,c3_delta
  ) 
  SELECT CAST(tmp.gts AS SIGNED) as groupNanoTs,tmp.cnt,tmp.ts_prev as deltaNanoTs, FROM_UNIXTIME(tmp.gts/1e9) as groupTimestamp,
    tmp.c1_peak,tmp.c1_cumul,if(tmp.ts_prev < 0,NULL,tmp.c1_cumul-tmp.c1_prev) as c1_delta,  
    tmp.c2_peak,tmp.c2_cumul,if(tmp.ts_prev < 0,NULL,tmp.c2_cumul-tmp.c2_prev) as c2_delta,  
    tmp.c3_peak,tmp.c3_cumul,if(tmp.ts_prev < 0,NULL,tmp.c3_cumul-tmp.c3_prev) as c3_delta 
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
                @prev_ts := IFNULL((select (t.recordNanoTs DIV @grpsec)*@grpsec+@grpsec FROM aMbusMC302Record t WHERE id = '67285016' AND recordNanoTs < @ts ORDER BY rowId DESC LIMIT 1),-1)
                ,@prev_c1_cumul := IFNULL((select t.heatingEnergy FROM aMbusMC302Record t WHERE id = '67285016' AND recordNanoTs < @ts ORDER BY rowId DESC LIMIT 1),-1)
                ,@prev_c2_cumul := IFNULL((select t.heatingEnergy FROM aMbusMC302Record t WHERE id = '67285015' AND recordNanoTs < @ts ORDER BY rowId DESC LIMIT 1),-1)
                ,@prev_c3_cumul := IFNULL((select t.heatingEnergy FROM aMbusMC302Record t WHERE id = '67280331' AND recordNanoTs < @ts ORDER BY rowId DESC LIMIT 1),-1)
                FROM (select @ts := ;REPLACEWITHNANOSEC;, @grpsec := 60e9, @maxts := (UNIX_TIMESTAMP() DIV 60)*60e9) TS 
                ) SQLVars
    ) tmp
    ;