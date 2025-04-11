# ------------------------- DATA MANAGER COMPONENT -------------------------
# Handles decoded data received from the Messages Manager Component.
# Keeps track (through a queue) of the order in which patient data (LISM) comes in.
# Updates the databases with the received data (ensuring new patients are correctly added,
# admission/discharge status is changed for present patients, new blood tests are added).
# Sends ready patient data (mrn, sex, age, test results and test dates) to the Prediction System.
# Handles edge cases when blood test rsults (LISM messages) come in before PAS messages.

import logging
from database_system import AKIDatabase 
from prometheus_client import Counter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class DataManager:
    """
    Data Manager component. Handles decoded data received from the Messages Manager.
    parameters:
        - db: AKI Database which stores patient info and test result data (new and historical)
        - LISM_queue: dictionary for LISM messages with matching patient in the database
        - pending_LISM: dictionary for LISM messages that have no matching patient in the database (edge cases)
        - ready_patient_data: queue for patient data to request prediction for
        - received_test_results: (counter metric) for total received test results
    """
    def __init__(self, DB_FILLED):
        self.db = AKIDatabase(DB_FILLED=DB_FILLED)
        self.LISM_queue = {}
        self.pending_LISM = {}
        self.ready_patient_data = []
        self.received_test_results = Counter("received_test_results","Number of test results received")

    def handle_patient_data(self, patient_id, event, message):
        """
        Handle patient data received from messages and add to the corresponding queue (PAS or LISM).
        inputs:
            - patient_id: MRN of the patient
            - event: type of message ('admitted' or 'discharged' for PAS, 'test_result' for LISM)
            - message: data from the message:
                - for PAS: {'name': str, 'age': int, 'sex': 'F'/'M}
                - for LISM: {"tests": test_results (float), "test_time": test_time}
        outputs:
            - (list): list of patient data ready for prediction, with tuples of (patient_data, test_result, historical_tests)
        """
        self.add_to_corresponding_queue(patient_id, event, message)
        self.process_queues()
        # print if multiple patients are ready for prediction
        if len(self.ready_patient_data) > 1:
            print(f"Ready Patient Data: {len(self.ready_patient_data)}\n")
        return self.ready_patient_data

    def add_to_corresponding_queue(self, patient_id, event, message):
        """
        Add patient data to the right queue based on message order and type.
        It updates the patient status in the datatabse for PAS messages.
        Ignores void or invalid messages.
        inputs:
            - patient_id: MRN of the patient
            - event: type of message ('admitted' or 'discharged' for PAS, 'test_result' for LISM)
            - message: data from the message:
                - for PAS: {'name': str, 'age': int, 'sex': 'F'/'M}
                - for LISM: {"tests": {test_results (float)}, "test_time": {test_time}}
        outputs: /
        """
        if message == 'error':
            logging.error(f"Error: Invalid message {message}.\n")
        elif (patient_id is None) or (patient_id == 'error'):
            logging.error("Error: No patient ID found in message.\n")
        else:
            if event == "admitted":
                self.db.insert_patient(patient_id, name=message['name'], age=message['age'], sex=message['sex'], admission_status='admitted')
                if patient_id in self.pending_LISM.keys():
                    self.LISM_queue[patient_id] = self.pending_LISM[patient_id]
                    del self.pending_LISM[patient_id]
            elif event == "discharged":
                self.db.update_patient_status(patient_id, 'discharged')
                if patient_id in self.pending_LISM.keys():
                    self.LISM_queue[patient_id] = self.pending_LISM[patient_id]
                    del self.pending_LISM[patient_id]
            elif event == "test_result":
                # keep track of received test results
                self.received_test_results.inc()
                if self.db.patient_exists(patient_id):
                    self.LISM_queue[patient_id] = {'mrn': patient_id, 'test_time': message['test_time'], 'test_result': message['tests']}
                else:
                    self.pending_LISM[patient_id] = {'mrn': patient_id, 'test_time': message['test_time'], 'test_result': message['tests']}
            else:
                logging.error("Error: Invalid message type.\n")

    def remove_from_ready_queue(self, patient_data, test_result, historical_tests):
        """
        Remove patient data from the ready queue after prediction has been completed.
        inputs:
            - patient_data: dictionary with patient data (MRN, age, sex)
            - test_result: dictionary with the last test result(s) and test date(s)
            - historical_tests: list of dictionaries with historical test results and dates
        outputs: /
        """
        self.ready_patient_data.remove((patient_data, test_result, historical_tests))

    def process_queues(self):
        """
        Process the PAS and LISM queues to find matching patient data. If both PAS and LISM messages
        have been sent for the same patient, adds the patient data in the ready queue for prediction.
        inputs: /
        outputs: /
        """
        remove_from_pending = []
        for mrn in self.LISM_queue.keys():
            pas = self.db.get_patient(mrn) # retrieve patient data from database
            patient_data = {'mrn': pas[0], 'name': pas[1], 'age': pas[2], 'sex': pas[3]}
            test_result = self.LISM_queue[mrn]
            historical_tests = self.process_ready_patient_data(patient_data, test_result) # get historical tests from database
            self.ready_patient_data.append((patient_data, test_result, historical_tests))
            remove_from_pending.append(mrn)
        
        # remove processed patients from the LISM queue
        for mrn in remove_from_pending:
            del self.LISM_queue[mrn]

    def process_ready_patient_data(self, patient_data, test_result):
        """
        Process patient data and test results for a patient that is ready for prediction. Adds patient to database
        (if not already there), or changes its admission status. It retrieves historical test results from the database
        for that patient. It adds the latest received blood test to the database.
        inputs:
            - patient_data: dictionary with patient data (MRN, age, sex)
            - test_result: dictionary with the last test result(s) and test date(s)
        outputs:
            - historical_tests: list of dictionaries with historical test results and dates for that patient
        """
        historical_tests = self.get_history_from_database(patient_data['mrn'])
        if len(test_result['test_result']) > 1:
            # print(len(test_result), "test results for patient", patient_data['mrn'])
            for test_num in range(len(test_result['test_result'])):
                self.db.insert_blood_test(patient_data['mrn'],test_result['test_time'][test_num], test_result['test_result'][test_num])
        else:
            self.db.insert_blood_test(patient_data['mrn'],test_result['test_time'], test_result['test_result'][0])
        return historical_tests
        
    def get_history_from_database(self, mrn):
        """
        Retrieve historical test results for a patient (by MRN) from the database.
        inputs:
            - mrn: MRN of the patient
        outputs:
            - (list): list of dictionaries with historical test results and dates for that patient
        """
        historical_tests = self.db.get_patient_tests_by_mrn(mrn)
        header = ['test_time', 'test_result']
        zipped_data = [dict(zip(header, i)) for i in historical_tests]
        return zipped_data
    
    def close_connection(self):
        """
        Close the connection to the database.
        inputs: /
        outputs: /
        """
        self.db.close()
    
    def diagnostics(self):
        """
        Print diagnostics information about the DataManager component.
        inputs: /
        outputs: /
        """
        print("Data Manager Diagnostics:\n    Received Test Results:", int(self.received_test_results._value.get()))
