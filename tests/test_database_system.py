import os
import sqlite3
import tempfile
import unittest
from io import StringIO
from unittest.mock import mock_open, patch

import sys

# Dynamically add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from system.database_system import AKIDatabase
# -------------------------
# BEGIN TESTS
# -------------------------

class TestAKIDatabaseUnit(unittest.TestCase):
    """Unit tests for individual AKIDatabase methods."""

    def setUp(self):
        # Create a temporary file to be used as the database.
        temp_db_file = tempfile.NamedTemporaryFile(delete=False)
        temp_db_file.close()  # Close the file so SQLite can open it.
        self.db_path = temp_db_file.name
        self.db = AKIDatabase(db_name=self.db_path)

    def tearDown(self):
        # Clean up: close the DB connection and delete the temporary file.
        self.db.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_create_tables(self):
        """Test that the required tables are created."""
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = {row[0] for row in cursor.fetchall()}
            self.assertIn("patients", tables)
            self.assertIn("blood_tests", tables)

    def test_insert_patient_and_get_patient(self):
        """Test inserting a patient and then retrieving it."""
        self.db.insert_patient(12345, "John Doe", 65, "M", "admitted")
        patient = self.db.get_patient(12345)
        self.assertIsNotNone(patient)
        # Expected tuple: (mrn, name, age, sex, admission_status)
        self.assertEqual(patient[0], 12345)
        self.assertEqual(patient[1], "John Doe")
        self.assertEqual(patient[2], 65)
        self.assertEqual(patient[3], "M")
        self.assertEqual(patient[4], "admitted")

        # Update the same patient with new information.
        self.db.insert_patient(12345, "John Doe", 66, "M", "discharged")
        patient = self.db.get_patient(12345)
        self.assertEqual(patient[2], 66)
        self.assertEqual(patient[4], "discharged")

    def test_insert_blood_test_and_get_tests(self):
        """Test inserting blood tests and retrieving them (ordered by date descending)."""
        # Insert a patient first.
        self.db.insert_patient(12345, "John Doe", 65, "M", "admitted")
        # Insert two blood tests.
        self.db.insert_blood_test(12345, "2025-02-04 12:00:00", 1.9)
        self.db.insert_blood_test(12345, "2025-02-03 12:00:00", 1.8)
        tests = self.db.get_patient_tests_by_mrn(12345)
        self.assertEqual(len(tests), 2)
        # Tests should be ordered by test_date descending.
        self.assertEqual(tests[0][0], "2025-02-04 12:00:00")
        self.assertEqual(tests[0][1], 1.9)
        self.assertEqual(tests[1][0], "2025-02-03 12:00:00")
        self.assertEqual(tests[1][1], 1.8)

    def test_update_patient_status(self):
        """Test updating a patient’s admission status."""
        self.db.insert_patient(12345, "John Doe", 65, "M", "admitted")
        self.db.update_patient_status(12345, "discharged")
        patient = self.db.get_patient(12345)
        self.assertEqual(patient[4], "discharged")

    def test_insert_history(self):
        """Test inserting historical blood test data from a CSV file."""
        # Prepare a CSV string matching the expected format:
        # Header: MRN,Date,Creatinine,Date,Creatinine
        # One row with two blood test entries for MRN 12345.
        csv_content = (
            "MRN,Date,Creatinine,Date,Creatinine\n"
            "12345,2025-02-01,1.2,2025-02-02,1.3\n"
        )
        m = mock_open(read_data=csv_content)
        with patch("builtins.open", m):
            self.db.insert_history("dummy_history.csv")

        # Verify that the blood_tests table has two new rows for MRN 12345 with test_source 'historical'.
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT test_date, creatinine_level, test_source FROM blood_tests WHERE mrn = ?",
                (12345,)
            )
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 2)
            for test_date, creatinine, test_source in rows:
                self.assertIn(test_date, ["2025-02-01", "2025-02-02"])
                self.assertIn(creatinine, [1.2, 1.3])
                self.assertEqual(test_source, "historical")


class TestAKIDatabaseIntegration(unittest.TestCase):
    """Integration tests that simulate a realistic patient workflow."""

    def setUp(self):
        temp_db_file = tempfile.NamedTemporaryFile(delete=False)
        temp_db_file.close()
        self.db_path = temp_db_file.name
        self.db = AKIDatabase(db_name=self.db_path)

    def tearDown(self):
        self.db.close()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_complete_flow(self):
        """
        Integration test: simulate adding a patient, inserting new blood tests,
        updating status, and then inserting historical data.
        """
        # Insert a new patient.
        self.db.insert_patient(11111, "Alice Smith", 70, "F", "admitted")
        # Insert two new blood tests.
        self.db.insert_blood_test(11111, "2025-02-05 10:00:00", 1.4)
        self.db.insert_blood_test(11111, "2025-02-06 09:00:00", 1.6)
        # Update patient status.
        self.db.update_patient_status(11111, "discharged")
        # Retrieve patient info and tests.
        patient = self.db.get_patient(11111)
        tests = self.db.get_patient_tests_by_mrn(11111)
        self.assertEqual(patient[4], "discharged")
        self.assertEqual(len(tests), 2)

        # Now simulate inserting historical blood test data from a CSV.
        csv_content = "MRN,Date,Creatinine\n11111,2025-02-01,1.2\n"
        m = mock_open(read_data=csv_content)
        with patch("builtins.open", m):
            self.db.insert_history("dummy_history.csv")

        # Verify that the historical record was added.
        with self.db.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM blood_tests WHERE mrn = ? AND test_source = 'historical'",
                (11111,)
            )
            count = cursor.fetchone()[0]
            self.assertEqual(count, 1)


if __name__ == '__main__':
    unittest.main()
