# ------------------------- DATABASE COMPONENT -------------------------
# SQL Database to store patient information and blood test results.
# Initialised with historical data from a CSV file.

import sqlite3
import os
from typing import Optional, List, Tuple
import csv
import logging

DB_PATH = os.getenv('DB_PATH', "../data/hospital_aki.db") # persistent storage
HISTORY_PATH = os.getenv('HISTORY_PATH', "../data/history.csv")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class AKIDatabase:
    """
    SQL database for storing patient information and blood test results.
    Initialised with historical data from a CSV file.
    class parameters:
        - db_name: path to the database file
    main functions:
        - connect(): connect to the SQLite database
        - create_tables(): initialise the database tables (patient and tests)
        - insert_history(): insert historical data from a CSV file
        - insert_patient(): insert or update a patient record
        - insert_blood_test(): insert a new blood test record
        - update_patient_status(): update the admission status of a patient
        - get_patient(): retrieve patient information by MRN
        - get_patient_tests_by_mrn(): retrieve all blood test records for a patient by their MRN
    """
    def __init__(self, db_name: str = DB_PATH, DB_FILLED: bool = False):
        self.db_name = db_name
        if not DB_FILLED:
            logging.info(f"Loading historical data from {HISTORY_PATH} into the database...")
            self.create_tables()
            self.insert_history(HISTORY_PATH)
            # print("Database successfully initialised with historical data")
        else:
            logging.info(f"Loading pre-saved database at {DB_PATH}.")
            self.create_tables()

    def connect(self):
        """
        Connects to the SQLite database.
        """
        return sqlite3.connect(self.db_name)

    def create_tables(self):
        """
        Creates PATIENT table for patient data and BLOOD_TEST table for blood test data.
        """
        with self.connect() as conn:
            cursor = conn.cursor()

            # Create Patients Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS patients (
                    mrn INTEGER PRIMARY KEY,
                    name TEXT,
                    age INTEGER,
                    sex TEXT,
                    admission_status TEXT
                )
            """)

            # Create Blood Test Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS blood_tests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    mrn INTEGER,
                    test_date TEXT,
                    creatinine_level REAL,
                    test_source TEXT CHECK(test_source IN ('historical', 'new')),
                    FOREIGN KEY (mrn) REFERENCES patients(mrn)
                )
            """)

            conn.commit()

    def insert_history(self, filename: str):
        """
        Adds historical data from a CSV file to the database.
        inputs:
            - filename: name of the CSV file containing historical data
        outputs: /
        """
        with self.connect() as conn:
            cursor = conn.cursor()

            # Open CSV file and read data
            with open(filename, mode="r", newline="", encoding="utf-8") as file:
                csv_reader = csv.reader(file)
                header = next(csv_reader)  # Skip header row
                # Insert each row into the database
                for i,row in enumerate(csv_reader):
                    mrn = row[0]
                    for col in range(1, len(row), 2):
                        if row[col] == "":
                            break

                        cursor.execute("""
                            INSERT INTO blood_tests (mrn, test_date, creatinine_level, test_source)
                            VALUES (?, ?, ?, 'historical')
                        """, (mrn,row[col],row[col+1]))  # Assuming CSV columns are [MRN, Date, Creatinine Level]
            # Commit
            conn.commit()
        logging.info("Historical data successfully loaded into the database")
            

    def insert_patient(self, mrn: int, name: str, age: int, sex: str, admission_status: str = "admitted"):
        """
        Inserts a patient record to the database. If the patient is already in,
        it updates their record.
        inputs:
            - mrn: patient's Medical Record Number (MRN)
            - name: patient's name and surname
            - age: patient's age
            - admission_status: Patient's admission status. Deafults to 'admitted' for new patients
        outputs: /
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO patients (mrn, name, age, sex, admission_status)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(mrn) DO UPDATE SET 
                    name = excluded.name,
                    age = excluded.age,
                    sex = excluded.sex,
                    admission_status = excluded.admission_status
            """, (mrn, name, age, sex, admission_status))
            conn.commit()

    def insert_blood_test(self, mrn: int, test_date: str, creatinine_level: float, test_source: str = "new"):
        """
        Inserts a new blood test record for a patient based on their Medical Record Number (MRN).
        inputs:
            - mrn: patient's Medical Record Number (MRN)
            - test_date: date and time of the test in "YYYY-MM-DD HH:MM:SS" format
            - creatinine_level: patient's creatinine level
            - test_source: source of the test (historical or new)
        outputs: /
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO blood_tests (mrn, test_date, creatinine_level)
                VALUES (?, ?, ?)
            """, (mrn, test_date, creatinine_level))
            conn.commit()

    def update_patient_status(self, mrn: int, admission_status: str):
        """
        Updates the admission status of a patient.
        inputs:
            - mrn: patient's Medical Record Number (MRN)
            - admission_status: new admission status ('admitted' or 'discharged')
        outputs: /
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE patients SET admission_status = ?
                WHERE mrn = ?
            """, (admission_status, mrn))
            conn.commit()

    def get_patient(self, mrn: int) -> Optional[Tuple[int, str, int, str, str]]:
        """
        Retrieves patient information by MRN.
        inputs:
            - mrn: patient's Medical Record Number (MRN)
        outputs:
            - (Tuple): tuple containing patient information:
                        - patient's Medical Record Number (MRN)
                        - patient's name and surname
                        - patient's age
                        - patient's sex
                        - admission status
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM patients WHERE mrn = ?", (mrn,))
            return cursor.fetchone()

    def get_patient_tests_by_mrn(self, mrn: int) -> List[Tuple[str, float]]:
        """
        Retrieves all blood test records for a given patient.
        inputs:
            - mrn: patient's Medical Record Number (MRN)
        outputs:
            - (List): list of tuples containing blood test records:
                        - test date
                        - creatinine level
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT test_date, creatinine_level FROM blood_tests
                WHERE mrn = ?
                ORDER BY test_date DESC
            """, (mrn,))
            return cursor.fetchall()

    def get_patient_tests_historical(self) -> List[Tuple[str, float]]:
        """
        Retrieves all blood test records for every patient in the database.
        inputs: /
        outputs:
            - (List): list of tuples containing blood test records for all patients:
                        - test date
                        - creatinine level
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT test_date, creatinine_level FROM blood_tests
                WHERE test_source = "historical"
                ORDER BY test_date DESC
            """)
            return cursor.fetchall()

    def get_all_patients(self) -> List[Tuple[int, str, int, str, str]]:
        """
        Retrieves all information of every patients from the database.
        inputs: /
        outputs:
            - (List): list of tuples containing patient information:
                        - patient's Medical Record Number (MRN)
                        - patient's name and surname
                        - patient's age
                        - patient's sex
                        - patient's admission status
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM patients")
            return cursor.fetchall()
        
    def patient_exists(self, mrn: int) -> bool:
        """
        Checks if a patient is in the database.
        inputs: /
        outputs:
            - (bool): True if patient is in the database, False otherwise
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM patients WHERE mrn = ?", (mrn,))
            return True if cursor.fetchone() else False

    def close(self):
        """
        Close the database connection.
        """
        logging.info(f"Saved Database to {DB_PATH}. ")
        self.connect().close()
        logging.info(f"Closed database connection.")
