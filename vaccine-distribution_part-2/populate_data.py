import sys

# pandas to manipulate SQL answer set
import pandas as pd
import time, os
from datetime import timedelta

# for SQLite and other RDBMS
from sqlalchemy import create_engine, event, schema, Table, text
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker


def read_given_file(f, use_sem_col):
    # reads CSV file into a pandas dataframe
    if use_sem_col:
        return pd.read_csv(f, sep=";", comment="#", dtype="unicode")
    else:
        return pd.read_csv(f, sep=",", comment="#", dtype="unicode")

def convert_to_date(column):
    # Transform into dates all the strings with a correct date format (or NaT)
    dates = pd.to_datetime(column, errors='coerce').dt.date
    # Convert into numbers all the strings containing integers or floats
    numbers = pd.to_numeric(column, errors='coerce')
    # Excel's numeric form of dates is the number of days since 1.1.1900 plus
    # 2 days, so it is used to convert any possible unformatted date to a date
    numeric_dates = \
        pd.to_datetime(numbers, unit='D', origin=pd.Timestamp('1900-01-01'),
                       errors='coerce').dt.date - timedelta(days=2)
    # Substitute those NaT that were unformatted dates with the correct date
    return dates.where(~dates.index.isin(numeric_dates.dropna().index),
                       numeric_dates.values).values

try:
    current_path = "//home//cgrp15//project_part_2//"
    data_path = current_path + "data//"
    TXT_ = ".txt"
    CSV_ = ".csv"
    QTS_ = '"'
    SLASH_ = "\\"
    # table
    manufacturer = "Manufacturer"
    diagnosis = "Diagnosis"
    patient = "Patients"
    shifts = "Shifts"
    staffmember = "StaffMembers"
    symptoms = "Symptoms"
    transportationLog = "Transportation log"
    vaccinations = "Vaccinations"
    vaccinationStations = "VaccinationStations"
    vaccineBatch = "VaccineBatch"
    vaccinePatients = "VaccinePatients"
    vaccineType = "VaccineType"
    SUPPRESS_QTS = True
    SQLITE_SRV = "sqlite:///"
    DB_NAME_ = "vaccination_db.db3"

    engine = create_engine(SQLITE_SRV + current_path + DB_NAME_, echo=False)
    sqlite_conn = engine.connect()
    if not sqlite_conn:
        print("DB connection is not OK!")
        exit()
    else:
        print("DB connection is OK.")

    # ------------------------------------
    # R E A D I N G  C S V   F I L E S
    ##------------------------------------
    df_manufacturer = read_given_file(data_path + manufacturer + CSV_, False)
    df_manufacturer = df_manufacturer.loc[
        :, ~df_manufacturer.columns.str.contains("^Unnamed")
    ]
    df_manufacturer = df_manufacturer.dropna(how='all')
    df_manufacturer.rename(columns={"vaccine": "vaccineID"}, inplace=True)
    manufacturer_sqltbl = "Manufacturer"
    df_manufacturer.to_sql(
        manufacturer_sqltbl, sqlite_conn, if_exists="append", index=False
    )

    df_diagnosis = read_given_file(data_path + diagnosis + CSV_, False)
    df_diagnosis['date'] = convert_to_date(df_diagnosis['date'])
    df_diagnosis = df_diagnosis.dropna(how='all')
    diagnosis_sqltbl = "Diagnosis"
    df_diagnosis.to_sql(diagnosis_sqltbl, sqlite_conn, if_exists="append", index=False)

    df_patient = read_given_file(data_path + patient + CSV_, False)
    df_patient = df_patient.loc[:, ~df_patient.columns.str.contains("^Unnamed")]
    df_patient = df_patient.dropna(how='all')
    df_patient.rename(columns={"date of birth": "dateOfBirth"}, inplace=True)
    df_patient['dateOfBirth'] = convert_to_date(df_patient['dateOfBirth'])
    df_patient = df_patient.dropna(how='all')
    patient_sqltbl = "Patient"
    df_patient.to_sql(patient_sqltbl, sqlite_conn, if_exists="append", index=False)

    df_vaccine = read_given_file(data_path + vaccineType + CSV_, False)
    df_vaccine = df_vaccine.loc[:, ~df_vaccine.columns.str.contains("^Unnamed")]
    df_vaccine = df_vaccine.dropna(how='all')
    vaccine_sqltbl = "Vaccine"
    df_vaccine.to_sql(vaccine_sqltbl, sqlite_conn, if_exists="append", index=False)

    df_vaccineBatch = pd.read_csv(
        data_path + vaccineBatch + CSV_, sep=",", encoding="Latin-1"
    )
    df_vaccineBatch = df_vaccineBatch.loc[
        :, ~df_vaccineBatch.columns.str.contains("^Unnamed")
    ]
    df_vaccineBatch = df_vaccineBatch.drop("type", 1)
    df_vaccineBatch.rename(columns={"manufacturer": "manufacturerID"}, inplace=True)
    df_vaccineBatch['manufDate'] = convert_to_date(df_vaccineBatch['manufDate'])
    df_vaccineBatch['expiration'] = convert_to_date(df_vaccineBatch['expiration'])
    df_vaccineBatch = df_vaccineBatch.dropna(how='all')
    vaccineBatch_sqltbl = "VaccineBatch"
    df_vaccineBatch.to_sql(
        vaccineBatch_sqltbl, sqlite_conn, if_exists="append", index=False
    )

    df_symptom = read_given_file(data_path + symptoms + CSV_, False)
    symptom_sqltbl = "Symptom"
    df_symptom.to_sql(symptom_sqltbl, sqlite_conn, if_exists="append", index=False)

    df_staff = pd.read_csv(data_path + staffmember + CSV_, sep=",", encoding="Latin-1")
    df_staff.rename(
        columns={
            "social security number": "ssNo",
            "date of birth": "dateOfBirth",
            "vaccination status": "vaccinationStatus",
        },
        inplace=True,
    )
    df_staff['dateOfBirth'] = convert_to_date(df_staff['dateOfBirth'])
    df_staff = df_staff.dropna(how='all')
    staffmember_sqltbl = "Staff"
    df_staff.to_sql(staffmember_sqltbl, sqlite_conn, if_exists="append", index=False)

    df_transportationLog = pd.read_csv(
        data_path + transportationLog + CSV_, sep=",", encoding="Latin-1"
    )
    df_transportationLog = df_transportationLog.loc[
        :, ~df_transportationLog.columns.str.contains("^Unnamed")
    ]
    df_transportationLog.rename(columns={"departure ": "departure"}, inplace=True)
    df_transportationLog['dateArr'] = convert_to_date(df_transportationLog['dateArr'])
    df_transportationLog['dateDep'] = convert_to_date(df_transportationLog['dateDep'])
    df_transportationLog = df_transportationLog.dropna(how='all')
    transportationLog_sqltbl = "TransportationLog"
    df_transportationLog.to_sql(
        transportationLog_sqltbl, sqlite_conn, if_exists="append", index=False
    )

    df_vaccinationStations = pd.read_csv(
        data_path + vaccinationStations + CSV_, sep=",", encoding="Latin-1"
    )
    df_vaccinationStations = df_vaccinationStations.loc[
        :, ~df_vaccinationStations.columns.str.contains("^Unnamed")
    ]
    vaccinationStations_sqltbl = "Hospital"
    df_vaccinationStations.to_sql(
        vaccinationStations_sqltbl, sqlite_conn, if_exists="append", index=False
    )

    df_shifts = pd.read_csv(data_path + shifts + CSV_, sep=",", encoding="Latin-1")
    shifts_sqltbl = "Shift"
    df_shifts = df_shifts.loc[:, ~df_shifts.columns.str.contains("station")]
    df_shifts.to_sql(shifts_sqltbl, sqlite_conn, if_exists="append", index=False)

    df_vaccinations = pd.read_csv(
        data_path + vaccinations + CSV_, sep=",", encoding="Latin-1"
    )
    df_vaccinations = df_vaccinations.loc[
        :, ~df_vaccinations.columns.str.contains("^Unnamed")
    ]
    df_vaccinations.rename(columns={"location ": "location"}, inplace=True)
    df_vaccinations['date'] = convert_to_date(df_vaccinations['date'])
    df_vaccinations = df_vaccinations.dropna(how='all')
    vaccinations_sqltbl = "VaccinationEvent"
    df_vaccinations.to_sql(
        vaccinations_sqltbl, sqlite_conn, if_exists="append", index=False
    )

    df_vaccinePatients = pd.read_csv(
        data_path + vaccinePatients + CSV_, sep=",", encoding="Latin-1"
    )
    df_vaccinePatients = df_vaccinePatients.loc[
        :, ~df_vaccinePatients.columns.str.contains("^Unnamed")
    ]
    df_vaccinePatients = df_vaccinePatients.dropna(how='all')
    vaccinePatients_sqltbl = "PatientVaccinationEvent"
    df_vaccinePatients.to_sql(
        vaccinePatients_sqltbl, sqlite_conn, if_exists="append", index=False
    )
except Exception as e:
    print("FAILED due to:" + str(e))
finally:
    sqlite_conn.close()
# END

