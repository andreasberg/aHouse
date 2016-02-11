
DROP TABLE aPower;
CREATE TABLE aPower (
	id INTEGER PRIMARY KEY NOT NULL,
    ts TEXT NOT NULL, /* %Y%m%dT%H%M%f */
    circuit9_cumul INTEGER,
    circuit10_cumul INTEGER,
    circuit11_cumul INTEGER,
    circuit12_cumul INTEGER,
    circuit13_cumul INTEGER,
    circuit14_cumul INTEGER,
    circuit15_cumul INTEGER,
    circuit16_cumul INTEGER,
    circuit9_curr INTEGER,
    circuit10_curr INTEGER,
    circuit11_curr INTEGER,
    circuit12_curr INTEGER,
    circuit13_curr INTEGER,
    circuit14_curr INTEGER,
    circuit15_curr INTEGER,
    circuit16_curr INTEGER
);


