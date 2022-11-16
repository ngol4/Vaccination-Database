CREATE TABLE Manufacturer (
    ID TEXT PRIMARY KEY,
    country VARCHAR(100),
    phone VARCHAR(30),
    vaccineID TEXT NOT NULL,
    FOREIGN KEY (vaccineID) REFERENCES Vaccine(ID)
    ON UPDATE CASCADE
);

CREATE TABLE Vaccine (
    ID TEXT PRIMARY KEY,
    name TEXT,
    doses TINYINT NOT NULL,
    tempMin TINYINT,
    tempMax TINYINT
);

CREATE TABLE VaccineBatch (
    batchID TEXT PRIMARY KEY,
    amount INT NOT NULL,
    manufacturerID TEXT NOT NULL,
    manufDate DATE,
    location TEXT,
    expiration DATE NOT NULL,
    FOREIGN KEY (manufacturerID) REFERENCES Manufacturer(ID)
    ON UPDATE CASCADE,
    FOREIGN KEY (location) REFERENCES Hospital(name)
    ON UPDATE CASCADE
);

CREATE TABLE TransportationLog (
    batchID TEXT,
    arrival TEXT NOT NULL,
    departure TEXT NOT NULL,
    dateArr DATE NOT NULL,
    dateDep DATE NOT NULL,
    PRIMARY KEY (batchID, dateArr),
    FOREIGN KEY (batchID) REFERENCES VaccineBatch(batchID)
    ON UPDATE CASCADE,
    FOREIGN KEY (arrival) REFERENCES Hospital(name)
    ON UPDATE CASCADE,
    FOREIGN KEY (departure) REFERENCES Hospital(name)
    ON UPDATE CASCADE
);

CREATE TABLE Hospital (
    name TEXT PRIMARY KEY,
    address TEXT,
    phone VARCHAR(30)
);

CREATE TABLE Staff (
    ssNo CHAR(13) PRIMARY KEY,
    name TEXT,
    dateOfBirth DATE,
    phone VARCHAR(30),
    role VARCHAR(15) NOT NULL,
    vaccinationStatus INT(1) NOT NULL,
    hospital TEXT,
    CHECK(vaccinationStatus IN (1, 0)),
    CHECK(role IN ('nurse', 'doctor')),
    FOREIGN KEY (hospital) REFERENCES Hospital(name)
    ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE TABLE Shift (
    weekday VARCHAR(12),
    worker CHAR(13), 
    CHECK (weekday IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')),
    PRIMARY KEY (weekday, worker),
    FOREIGN KEY (worker) REFERENCES Staff(ssNo)
    ON UPDATE CASCADE
);

CREATE TABLE VaccinationEvent (
    date DATE,
    location TEXT,
    batchID TEXT NOT NULL,
    PRIMARY KEY (location, date),
    FOREIGN KEY (location) REFERENCES Hospital(name)
    ON UPDATE CASCADE,
    FOREIGN KEY (batchID) REFERENCES VaccineBatch(batchID)
    ON UPDATE CASCADE
);

CREATE TABLE Patient (
    ssNo CHAR (13) PRIMARY KEY,
    name TEXT NOT NULL,
    dateOfBirth DATE,
    gender CHAR (1) CHECK (gender IN ('F', 'M')) 
);

CREATE TABLE PatientVaccinationEvent (
    date DATE,
    location TEXT,
    patientSsNo CHAR(13),
    PRIMARY KEY (patientSsNo, date, location),
    FOREIGN KEY (patientSsNo) REFERENCES Patient(ssNo)
    ON UPDATE CASCADE,
    FOREIGN KEY (location) REFERENCES Hospital(name)
    ON UPDATE CASCADE,
    FOREIGN KEY (date) REFERENCES VaccinationEvent(date)
    ON UPDATE CASCADE
);

CREATE TABLE Symptom (
    name TEXT PRIMARY KEY,
    criticality INT(1) CHECK (criticality IN (0, 1) ) 
);

CREATE TABLE Diagnosis (
    patient CHAR(13),
    symptom TEXT,
    date DATE,
    PRIMARY KEY (patient, symptom, date),
    FOREIGN KEY (patient) REFERENCES Patient(ssNo)
    ON UPDATE CASCADE,
    FOREIGN KEY (symptom) REFERENCES Symptom(name)
    ON UPDATE CASCADE
);
