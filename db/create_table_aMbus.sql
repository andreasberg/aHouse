CREATE TABLE IF NOT EXISTS aMbusSlaveInfo (
    rowid INTEGER PRIMARY KEY NOT NULL,
    address INTEGER NOT NULL,       /* Mbus primary address*/
    id TEXT NOT NULL,               /* XML: /MBusData/SlaveInformation/Id */
    manufacturer TEXT NOT NULL,     /* XML: /MBusData/Manufacturer */
    version INTEGER,                /* XML: /MBusData/Version */
    productName TEXT,               /* XML: /MBusData/ProductName */
    medium TEXT,                    /* XML: /MBusData/Medium */
    accessNumber TEXT,              /* XML: /MBusData/AccessNumber */
    signature TEXT,                 /* XML: /MBusData/Signature */
    lastStatus TEXT,                /* XML: /MBusData/Status */
    rowCreatedTimestamp INTEGER NOT NULL,
    rowUpdatedTimestamp INTEGER,
    UNIQUE (address,id) ON CONFLICT REPLACE
);
CREATE INDEX IF NOT EXISTS idx_aMbusSlaveInfo ON aMbusSlaveInfo(address);


CREATE TABLE IF NOT EXISTS aMbusDataRecord (
    rowid INTEGER PRIMARY KEY NOT NULL,
    address INTEGER NOT NULL,       /* Mbus primary address*/
    id TEXT NOT NULL,               /* Mbus slave (SlaveInformation) id*/
    accessNumber INTEGER NOT NULL,  /* Mbus slave (SlaveInformation) accessNumber*/
    recordId INTEGER NOT NULL,      /* XML: /MBusData/Record@id */
    recordFrame TEXT,               /* XML: /MBusData/Record@frame (optional), not clear when this data is received but libmbus xml has reserved space for this */ 
    recordFunction TEXT NOT NULL,   /* XML: /MBusData/Record/Function */
    recordStorageNumber INTEGER,    /* XML: /MBusData/Record/StorageNumber */
    recordTariff TEXT,              /* XML: /MBusData/Record/Tariff (optional) */ 
    recordDevice TEXT,              /* XML: /MBusData/Record/Device (optional) */ 
    recordUnit TEXT NOT NULL,       /* XML: /MBusData/Record/Unit */ 
    recordValue INTEGER NOT NULL,   /* XML: /MBusData/Record/Value */ 
    recordTimestampRaw TEXT NOT NULL,   /* XML: /MBusData/Record/Timestamp */ 
    recordTimestamp TEXT NOT NULL,      /* Parsed from /MBusData/Record/Timestamp */ 
    rowCreatedTimestamp INTEGER NOT NULL,
    UNIQUE (address,id,recordId,recordTimestamp) ON CONFLICT IGNORE
);

CREATE INDEX IF NOT EXISTS idx_aMbusDataRecord1 ON aMbusDataRecord(address,id,recordId,recordTimestamp);
CREATE INDEX IF NOT EXISTS idx_aMbusDataRecord2 ON aMbusDataRecord(address,recordId,recordTimestamp);
CREATE INDEX IF NOT EXISTS idx_aMbusDataRecord3 ON aMbusDataRecord(recordTimestamp);



