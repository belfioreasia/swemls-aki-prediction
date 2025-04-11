import unittest
from unittest.mock import MagicMock
import sys
import os

# Dynamically add the parent directory to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from system.data_manager import DataManager  

class TestDataManager(unittest.TestCase):

    def setUp(self):
        """Set up a new DataManager instance before each test."""
        self.manager = DataManager()
        
        # Mock the database system
        self.manager.db = MagicMock()

    ## ------------------------ TEST: add_to_corresponding_queue ------------------------ ##
    
    def test_add_patient_admission_to_queue(self):
        """Test adding an admitted patient to the PAS queue."""
        patient_data = {'name': "John Doe", 'age': 45, 'sex': "M"}
        self.manager.add_to_corresponding_queue(123456, "admitted", patient_data)
        
        self.manager.db.insert_patient.assert_called_once_with(123456, name="John Doe", age=45, sex="M", admission_status='admitted')

    def test_add_patient_discharge_to_queue(self):
        """Test handling patient discharge in the database."""
        self.manager.add_to_corresponding_queue(654321, "discharged", None)
        
    def test_add_test_result_to_queue(self):
        """Test adding a test result to the LISM queue."""
        test_data = {"tests": [1.2], "test_time": "2025-02-03 10:00:00"}
        self.manager.add_to_corresponding_queue(987654, "test_result", test_data)

        self.assertEqual(len(self.manager.pending_LISM), 1)
        self.assertEqual(self.manager.pending_LISM[987654]['test_result'], [1.2])

    def test_add_invalid_message_type(self):
        """Test handling of an invalid event type."""
        self.manager.add_to_corresponding_queue(123456, "invalid_event", {})

        self.assertEqual(len(self.manager.pending_LISM), 0)

    ## ------------------------ TEST: process_queues ------------------------ ##

    def test_process_queues_with_matching_data(self):
        """Test processing queues when PAS and LISM data match."""
        self.manager.db.patient_exists.return_value = True
        self.manager.LISM_queue[123456] = {'mrn': 123456, 'test_time': "2025-02-03 10:00:00", 'test_result': [1.2]}
        
        self.manager.process_queues()
        
        self.assertEqual(len(self.manager.ready_patient_data), 1)
        self.assertEqual(self.manager.ready_patient_data[0][1]['test_result'], [1.2])

    def test_process_queues_no_match(self):
        """Test processing queues when there is no matching PAS data."""
        self.manager.db.patient_exists.return_value = False
        self.manager.pending_LISM[123456] = {'mrn': 123456, 'test_time': "2025-02-03 10:00:00", 'test_result': [1.2]}
        
        self.manager.process_queues()
        
        self.assertEqual(len(self.manager.ready_patient_data), 0)
        self.assertEqual(len(self.manager.pending_LISM), 1)

    ## ------------------------ TEST: get_history_from_database ------------------------ ##
    
    def test_get_history_from_database(self):
        """Test retrieving historical test results from the database."""
        self.manager.db.get_patient_tests_by_mrn.return_value = [("2025-01-20 10:00:00", 1.0)]

        history = self.manager.get_history_from_database(123456)

        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]['test_time'], "2025-01-20 10:00:00")
        self.assertEqual(history[0]['test_result'], 1.0)
    

if __name__ == "__main__":
    unittest.main()
