CREATE TABLE IF NOT EXISTS aClimateDevices (
    rowid INTEGER PRIMARY KEY NOT NULL,
    deviceGroupType VARCHAR(40) NOT NULL,
    deviceGroupName VARCHAR(40),
    deviceId VARCHAR(40) NOT NULL,
    deviceName VARCHAR(40),
    submodule01Id VARCHAR(40),
    submodule01Name VARCHAR(40),
    submodule01TypeId VARCHAR(40),
    submodule01DataTypes VARCHAR(40),
    submodule02Id VARCHAR(40),
    submodule02Name VARCHAR(40),
    submodule02TypeId VARCHAR(40),
    submodule02DataTypes VARCHAR(40),
    submodule03Id VARCHAR(40),
    submodule03Name VARCHAR(40),
    submodule03TypeId VARCHAR(40),
    submodule03DataTypes VARCHAR(40),
    submodule04Id VARCHAR(40),
    submodule04Name VARCHAR(40),
    submodule04TypeId VARCHAR(40),
    submodule04DataTypes VARCHAR(40),
    submodule05Id VARCHAR(40),
    submodule05Name VARCHAR(40),
    submodule05TypeId VARCHAR(40),
    submodule05DataTypes VARCHAR(40),
    pressureUnit INTEGER,
    tempartureUnit INTEGER,
    windUnit INTEGER,
    locale VARCHAR(40),
    language VARCHAR(40),
    location VARCHAR(40),
    altitude INTEGER,
    lastMessageTimestamp INTEGER,
    lastSeenTimestamp INTEGER,
    confVersion INTEGER NOT NULL,
    confUpdatedTimestamp INTEGER NOT NULL,
    UNIQUE (deviceGroupType,deviceId,confVersion)
);

CREATE TABLE IF NOT EXISTS aClimateData (
    rowid INTEGER PRIMARY KEY NOT NULL,
    eventTimestamp VARCHAR(40) NOT NULL,
    sourceId VARCHAR(40) NOT NULL,
    eventType VARCHAR(40) NOT NULL,
    value DECIMAL(14,3) NOT NULL,
    locationType VARCHAR(40),   /* indoor,outdoor */
    sourceType VARCHAR(40),
    rowTimestamp VARCHAR(40) NOT NULL,
    UNIQUE (sourceType,sourceId,eventType,eventTimestamp) 
);
CREATE INDEX idx_aClimateData1 ON aClimateData (eventTimestamp,sourceId,eventType);
CREATE INDEX idx_aClimateData2 ON aClimateData (eventTimestamp,sourceId);
CREATE INDEX idx_aClimateData3 ON aClimateData (eventTimestamp);

