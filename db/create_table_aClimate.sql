CREATE TABLE IF NOT EXISTS aClimateDevices (
    rowid INTEGER PRIMARY KEY NOT NULL,
    deviceGroupType TEXT NOT NULL,
    deviceGroupName TEXT,
    deviceId TEXT NOT NULL,
    deviceName TEXT,
    submodule01Id TEXT,
    submodule01Name TEXT,
    submodule01TypeId TEXT,
    submodule01DataTypes TEXT,
    submodule02Id TEXT,
    submodule02Name TEXT,
    submodule02TypeId TEXT,
    submodule02DataTypes TEXT,
    submodule03Id TEXT,
    submodule03Name TEXT,
    submodule03TypeId TEXT,
    submodule03DataTypes TEXT,
    submodule04Id TEXT,
    submodule04Name TEXT,
    submodule04TypeId TEXT,
    submodule04DataTypes TEXT,
    submodule05Id TEXT,
    submodule05Name TEXT,
    submodule05TypeId TEXT,
    submodule05DataTypes TEXT,
    pressureUnit INTEGER,
    tempartureUnit INTEGER,
    windUnit INTEGER,
    locale TEXT,
    language TEXT,
    location TEXT,
    altitude INTEGER,
    lastMessageTimestamp INTEGER,
    lastSeenTimestamp INTEGER,
    confVersion INTEGER NOT NULL,
    confUpdatedTimestamp INTEGER NOT NULL,
    UNIQUE (deviceGroupType,deviceId,confVersion)
);

CREATE TABLE IF NOT EXISTS aClimateData (
    rowid INTEGER PRIMARY KEY NOT NULL,
    sourceType TEXT NOT NULL,
    sourceId TEXT NOT NULL,
    eventType TEXT NOT NULL,
    value INTEGER NOT NULL,
    locationType TEXT,   /* indoor,outdoor */
    eventTimestamp TEXT NOT NULL,
    rowTimestamp TEXT NOT NULL,
    UNIQUE (sourceType,sourceId,eventType,eventTimestamp) ON CONFLICT IGNORE  /* ignore attempt to insert over existing rows */
);
CREATE INDEX IF NOT EXISTS idx_aClimateData ON aClimateData(eventTimestamp);

CREATE TABLE IF NOT EXISTS aClimateDataDaily (
    rowid INTEGER PRIMARY KEY NOT NULL,
    sourceType TEXT NOT NULL,
    sourceId TEXT NOT NULL,
    eventType TEXT NOT NULL,
    eventGroupTimestamp TEXT NOT NULL,
    aggregationTimezone TEXT, /* timezone used for day start and end */
    minvalue INTEGER,
    avgvalue INTEGER,
    maxvalue INTEGER,
    eventCount INTEGER, /* number of events aggregated in row */
    locationType TEXT,   /* indoor,outdoor */
    rowTimestamp TEXT NOT NULL,
    UNIQUE (sourceType,sourceId,eventType,eventGroupTimestamp,aggregationTimezone) ON CONFLICT REPLACE  /* overwrite existing with new row */
);
CREATE INDEX IF NOT EXISTS idx_aClimateDataDaily ON aClimateDataDaily(eventGroupTimestamp);
CREATE INDEX IF NOT EXISTS idx_aClimateDataDaily1 ON aClimateDataDaily(sourceType,sourceId,eventType,eventGroupTimestamp,aggregationTimezone);

CREATE TRIGGER IF NOT EXISTS aClimateDataTrigger 
AFTER INSERT ON aClimateData 
WHEN NEW.eventType = 'Temperature' 
AND NEW.sourceId = '02-00-00-03-08-1c'
BEGIN 
INSERT OR REPLACE INTO aClimateDataDaily (
      sourceType,
      sourceId,
      eventType,
      eventGroupTimestamp,
      aggregationTimezone,
      minvalue,
      avgvalue,
      maxvalue,
      eventCount,
      locationType,
      rowTimestamp
      ) 
SELECT sourceType, sourceId, eventType,
      STRFTIME('%Y%m%d',datetime(eventTimestamp,'localtime')) AS eventGroupTimestamp, 
      'Europe/Helsinki' AS aggregationTimezone,
      MIN(value) AS minvalue,
      AVG(value) AS avgvalue,
      MAX(value) AS maxvalue,
      COUNT(value) AS eventCount,
      locationType,
      STRFTIME('%Y-%m-%d %H:%M:%f', 'NOW') AS rowTimestamp
FROM aClimateData 
WHERE eventGroupTimestamp = STRFTIME('%Y%m%d',datetime('now','localtime'))
      AND eventType = 'Temperature' 
      AND sourceId = '02-00-00-03-08-1c' /* default device Netatmo outdoor module */
      AND locationType = 'outdoor'
      AND sourceType = 'netatmo'
GROUP BY STRFTIME('%Y%m%d',datetime(eventTimestamp,'localtime'));
END;
