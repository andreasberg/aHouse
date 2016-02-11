CREATE TABLE IF NOT EXISTS aMbusMC302Record(
	rowId INTEGER PRIMARY KEY NOT NULL,
	accessNumber INTEGER NOT NULL, 	/* Slave MBus response sequence number */
	address INTEGER NOT NULL,   	/* Mbus primary address*/
	id TEXT NOT NULL,           	/* Mbus slave (SlaveInformation) id*/
	recordTimestampRaw TEXT, 	/* same for all data records in single Mbus response*/
	recordTimestamp TEXT,		/* parsed value of raw */
	heatingEnergy INTEGER,		/* Heating energy (cumulative, non-resettable) converted to Wh from 	1|Instantaneous value|0|Energy;100;Wh */
	coolingEnergy INTEGER, 		/* Cooling energy (cumulative, non-resettable) converted to Wh from 	2|Instantaneous value|0|Energy;100;Wh */
	energyM3T1 INTEGER,		/* Energy mˆ3 * T1 (cumulative Volume * Temperature) from 		3|Instantaneous value|0|Manufacturer specific */
	energyM3T2 INTEGER,		/* Energy mˆ3 * T2 (cumulative Volume * Temperature) from 		4|Instantaneous value|0|Manufacturer specific	*/
	volume INTEGER,			/* Current volume (cumulative converted to dm3)	from 			5|Instantaneous value|0|Volume;m;m^3 */
	hourCounter INTEGER,		/* Hour counter (non-resettable) from					6|Instantaneous value|0|On time (hours)	*/
	errorHourCounter INTEGER,	/* Error hour counter (cumul hours in error, no resettable) from	7|Value during error state|0|On time (hours) */
	temp1 REAL,			/* Current (Flow) Temperature T1 (converted to deg.Celsius)	from 	8|Instantaneous value|0|Flow temperature;1e-2;deg C */
	temp2 REAL,			/* Current (Return) Temperature T2 (converted to deg.Celsius) from	9|Instantaneous value|0|Return temperature;1e-2;deg C */
	deltaT1T2 REAL,			/* Temperature difference T1-T2 (converted to  deg.Celsius) from	10|Instantaneous value|0|Temperature Difference;1e-2;deg C */
	power INTEGER,			/* Current power (converted to W) from 					11|Instantaneous value|0|Power;100;W */
	powerMax INTEGER,		/* Maximum power since XX (converted to Watt) from 			12|Maximum value|0|Power;100;W */
	flow INTEGER,			/* Current water flow (converted to dm3/h) from				13|Instantaneous value|0|Volume flow;m;m^3/h  */
	flowMax INTEGER,		/* Maximum water flow since XX (converted to dm3/h)			14|Maximum value|0|Volume flow;m;m^3/h */
	errorFlags TEXT,		/* Error flags from							15|Instantaneous value|0|Error flags */
	timePoint TEXT,			/* Date+Time from 							16|Instantaneous value|0|Time Point (time & date) */
	targetHeatingEnergy	INTEGER,/* Heating energy since targetTimepoint	(in Wh)	from			17|Instantaneous value|1|Energy;100;Wh */
	targetCoolingEnergy INTEGER,	/* Cooling energy since targetTimepoint (in Wh) from			18|Instantaneous value|1|Energy;100;Wh */
	targetEnergyM3T1 INTEGER,	/* Energy mˆ3 * T1 since targetTimepoint from				19|Instantaneous value|1|Manufacturer specific */
	targetEnergyM3T2 INTEGER,	/* Energy mˆ3 * T2 since targetTimepoint from				20|Instantaneous value|1|Manufacturer specific */
	targetVolume INTEGER,		/* Volume since targetTimepoint (in dm3) from 				21|Instantaneous value|1|Volume;m;m^3 */
	targetPowerMax INTEGER,		/* Max power since targetTimepoint (in Watt) from  			22|Maximum value|1|Power;100;W 	*/
	targetFlowMax INTEGER,		/* Max flow since targetTimepoint (in dm3/h) from 			23|Maximum value|1|Volume flow;m;m^3/h 	*/
	targetTimepoint TEXT,		/* Target time point (date) from 					24|Instantaneous value|1|Time Point (date) */
	rowCreatedTimestamp TEXT,
	UNIQUE (accessNumber,id,recordTimestamp) ON CONFLICT REPLACE
);
