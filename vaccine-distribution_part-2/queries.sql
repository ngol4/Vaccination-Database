-- Query 1
SELECT DISTINCT Staff.ssNo, Staff.name, Staff.phone, Staff.role, Staff.vaccinationStatus, VaccinationEvent.location
FROM Staff JOIN VaccinationEvent JOIN Shift
ON Shift.worker = Staff.ssNo AND VaccinationEvent.location = Staff.hospital
WHERE Shift.weekday = "Monday" AND VaccinationEvent.date = "2021-05-10"

-- Query 2
SELECT DISTINCT Staff.name
FROM Staff
JOIN Hospital ON hospital = Hospital.name
JOIN Shift ON worker = ssNo
WHERE address LIKE "% HELSINKI" AND weekday = "Wednesday" AND role = "doctor"

-- Query 3
SELECT v.batchID, v.location current_location, t.arrival last_logged_location
FROM VaccineBatch v
INNER JOIN
(SELECT arrival, MAX(dateArr), batchID
FROM TransportationLog
GROUP BY batchID
) t ON v.batchID = t.batchID;

SELECT t.batchID, h.phone  
FROM VaccineBatch v
INNER JOIN
(SELECT arrival, MAX(dateArr), batchID
FROM TransportationLog
GROUP BY batchID
) t ON v.batchID = t.batchID
INNER JOIN Hospital h ON v.location = h.name
WHERE v.location <> t.arrival;

-- Query 4
SELECT DISTINCT Patient.name, VaccinationEvent.batchID, Vaccine.name, PatientVaccinationEvent.date, PatientVaccinationEvent.location
FROM PatientVaccinationEvent
JOIN Diagnosis ON patientSsNo = patient
JOIN Symptom ON symptom = Symptom.name
JOIN Patient ON patient = ssNo
JOIN VaccinationEvent ON VaccinationEvent.date = PatientVaccinationEvent.date AND PatientVaccinationEvent.location = VaccinationEvent.location
JOIN VaccineBatch ON VaccinationEvent.batchID = VaccineBatch.batchID
JOIN Manufacturer ON Manufacturer.ID = manufacturerID
JOIN Vaccine ON vaccineID = Vaccine.ID
WHERE criticality = 1 AND "2021-05-10" < Diagnosis.date
ORDER BY patientSsNo, VaccinationEvent.date

-- Query 5
CREATE VIEW VaccinationStatus AS
SELECT Patient.name Name, Patient.dateOfBirth DateOfBirth, Patient.gender Gender, Patient.ssNo SocialSecurityNumber,
CASE WHEN Vaccine.doses = COUNT(PatientVaccinationEvent.patientSsNo) THEN '1' ELSE '0' END AS vaccinationStatus
FROM Patient
LEFT JOIN PatientVaccinationEvent ON Patient.ssNo = PatientVaccinationEvent.patientSsNo
LEFT JOIN VaccinationEvent ON (PatientVaccinationEvent.location = VaccinationEvent.location) AND (PatientVaccinationEvent."date" = VaccinationEvent."date")
LEFT JOIN VaccineBatch  ON VaccinationEvent.batchID = VaccineBatch.batchID
LEFT JOIN Manufacturer ON VaccineBatch.manufacturerID = Manufacturer.ID
LEFT JOIN Vaccine ON Manufacturer.vaccineID = Vaccine.ID
GROUP BY Patient.ssNo;

-- Query 6
SELECT location AS hospital, name AS vaccine, total AS "number of vaccines of type", SUM(total) OVER (PARTITION BY location) AS "total number of vaccines" FROM (
  SELECT location, Vaccine.name, SUM(amount) AS total FROM VaccineBatch
  JOIN Manufacturer ON Manufacturer.ID = VaccineBatch.manufacturerID
  JOIN Vaccine ON Vaccine.ID = Manufacturer.vaccineID
  GROUP BY location, vaccineID
)

-- Query 7
WITH Raw AS
(
 SELECT PV.patientSsNo AS ssNo, V.name, symptom FROM PatientVaccinationEvent PV
 JOIN VaccinationEvent VE ON VE.date = PV.date AND VE.location = PV.location
 JOIN VaccineBatch VB ON VB.batchID = VE.batchID
 JOIN Manufacturer M ON M.ID = VB.manufacturerID
 JOIN Vaccine V ON V.ID = M.vaccineID
 JOIN Diagnosis D ON D.patient = PV.patientSsNo AND D.date > PV.date
),
Totals AS
(
 SELECT name, COUNT(DISTINCT(ssNo)) AS total FROM Raw GROUP BY name
),
ByType AS
(
 SELECT name, symptom, COUNT(DISTINCT(ssNo)) AS total FROM Raw GROUP BY name, symptom
)
SELECT ByType.name AS vaccine, ByType.symptom, ByType.total * 1.0 / Totals.total AS frequency FROM ByType JOIN Totals ON ByType.name = Totals.name
