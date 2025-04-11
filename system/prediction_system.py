# ------------------------- PREDICTION SYSTEM COMPONENT -------------------------
# Receives patient data (PAS and LISM) from the 'DataManager' component. It formats it
# to match the input format for the ML model to predict aki status.
# It then uses the pre-trained model to predict AKI and triggers the 'Alert System' component
# to page the clinitians if AKI is detected. It checks the status of the alert (received successfully
# or not by the Pager system) and retries if necessary.
# If test mode is enabled, it stores the list of paged patients (MRN, test date) in a CSV file for 
# system and model evaluation.

import os
import time
import logging
import http
import http.server
import requests
import pandas as pd
import joblib
from statistics import mean, median
from prometheus_client import Counter, Histogram

PAGER_ADDRESS = os.getenv('PAGER_ADDRESS', "host.docker.internal:8441")
PAGER_ALLOWED_TIMEOUT = 1
PAGER_ALLOWED_RETRIES = 2
PAGER_CONNECTION_DELAY = 0.5

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class AlertSystem(http.server.HTTPServer):
    """
    System Pager Alert component. Pages the clinitians if aki is detected and ensures the alert is
    either succesfully received by the clinitian or safely disregarded in case of connection error.
    parameters:
        - _num_pagers: (counter metric) number of alerts sent to pager
        - retried_messages: (counter metric) number of retried messages
        - error_alerts: Number of alerts that failed to page
        - _pager_url: URL for Pager connection (set as environment variable)
    """
    def __init__(self):
        self._num_pagers = Counter("alerted_patients","Number of patients alerted")
        self.retried_messages = Counter('retried_messages', 'Number of retried messages')
        self.error_alerts = Counter('error_alerts', 'Number of error alerts')
        self._pager_url = f"http://{PAGER_ADDRESS}/page"
        logging.info(f"Alert System: listening on {PAGER_ADDRESS}")

    def _check_post_health(self, status_code):
        """
        Check the status of the alert (received successfully or not by the Pager system).
        The pager will send:
            - "200 OK" response if the page was successful. 
            - 400 (Bad request) response if the sent request to the system isn't valid.
              Paging hasn't happened and no need to retry.
            - 500 (Internal server error) response if the pager system has failed.
              System will retry up to 2 times to page the clinitians.
        inputs:
            - status_code: status code from the HTTP response
        outputs: 
            - (str): signals action to take based on the status code:
                    - "OK": if the alert was successfully sent to the pager
                    - "IGNORE": if the alert was not sent to the pager, no rety necessay
                    - "RETRY": if the pager system has failed, system needs to retry
        """
        if status_code == 200:
            return "OK"
        elif status_code == 400:
            logging.error(f"     Bad System Request (Status code: {status_code})")
            return "IGNORE"
        elif status_code == 500:
            logging.error(f"     Pager System has failed. Retry Later (Status code: {status_code})")
            return "RETRY"
        else:
            logging.error(f"     Unhandled Response (Status code: {status_code})")
            return "IGNORE"

    def _post(self, patient_mrn, last_test_date):
        """
        Send alert to pager as HTTP post request. Sends the patient MRN and the last test date.
        inputs:
            - patient_mrn: MRN of the patient
            - last_test_date: date and time of the last test in YYYYMMDDHHMMSS format
        outputs:
            - (int): status of the alert (200/400/500/other)
        """
        # format date and time for paging (YY/MM/DD HH:MM:SS -> YYYYMMDDHHMMSS)
        last_test_date = last_test_date.replace(" ", "").replace("-", "").replace(":", "")
        data = f"{patient_mrn},{last_test_date}"
        logging.info(f"     Paging: {data}")
        try:
            response = requests.post(self._pager_url, data=data, headers={'Content-Type': 'text/plain'}, timeout=PAGER_ALLOWED_TIMEOUT) 
            post_status = self._check_post_health(response.status_code)
        except requests.Timeout:
            logging.info("     Connection timed out.")
            post_status = "RETRY"
        except Exception as e:
            logging.error(f"     Failed to send alert to pager ({e})")
            post_status = "RETRY"
        return post_status

    def trigger_alert(self, patient_mrn, last_test_date):
        """
        Start process to alert to pager if AKI is detected. Sends the patient MRN and the last test date.
        Based on the response from the pager system, it will retry to page the same patient up to 2 times.
        If reception has been successful, it will measure the latency of the alert system and will store
        the maximum latency taken (so far) by the system when sending alerts.
        inputs:
            - patient_mrn: MRN of the patient
            - last_test_date: date and time of the last test
        outputs: /
        """
        try:
            self._num_pagers.inc()

            post_status = self._post(patient_mrn, last_test_date)
            if post_status == "RETRY":
                self.retried_messages.inc()
                repaged_counts = 0
                # repage same patient at most twice
                while (post_status == "RETRY") and (repaged_counts < PAGER_ALLOWED_RETRIES):
                    repaged_counts += 1
                    logging.info(f"          Retrying alert in {PAGER_CONNECTION_DELAY:.2f}s (re-page {repaged_counts}).")
                    time.sleep(PAGER_CONNECTION_DELAY)
                    post_status = self._post(patient_mrn, last_test_date)
                if repaged_counts >= PAGER_ALLOWED_RETRIES:
                    logging.error("          Failed to page clinitians.")
                    # ingore if failed to re-page clinitians
                    post_status == "IGNORE"

            if post_status == "OK":
                logging.info("     Paging successful.")
            elif post_status == "IGNORE":
                logging.info("     Ignoring alert request.")
                self.error_alerts.inc()

        except Exception as e:
            logging.error("     Failed to send alert to pager")
            print(e)
            self.error_alerts.inc()

    
class PredictionSystem():
    """
    Prediction System component. Predicts AKI using the trained model and triggers the Alert System.
    parameters:
        - alert_system: AlertSystem object
        - total_predictions: (counter metric) number of patients sent to model for prediction
        - blood_test_dist: (histogram metric) distribution of patients' blood test results
        - model: pre-trained ML model for AKI prediction
        - test: boolean: if true enables test mode (system diagnostics and save paged patients)
        - _paged_list: list of paged patients (MRN, test date)
    """
    def __init__(self, test = False):
        self.alert_system = AlertSystem()
        self.total_predictions = Counter("total_predictions","Number of patients sent to model for prediction")
        self.positive_predictions = Counter("positive_predictions","Number of total AKI detections")
        self.blood_test_dist = Histogram("blood_test_distribution", "Distribution of blood test result values", buckets=[i for i in range(50, 150, 10)])
        self.test = test
        self.paged_list = []
        try:
            with open("model.pt", "rb") as f:
                self.model = joblib.load(f)
            logging.info("Model loaded successfully.")
        except Exception as e:
            logging.error("Model not found.")
            self.model = None
            raise Exception(f"Model not found ({e}).")

    def run(self, patient_data, test_result, historical_tests):
        """
        Request AKI prediction for a patient and trigger the Alert System (if aki detected,
        i.e. model prediction = 1). If the system is in TEST mode, it will store the paged patient.
        inputs:
            - patient_data: dictionary with patient data (MRN, age, sex)
            - test_result: dictionary with the last test result(s) and test date(s)
            - historical_tests: list of dictionaries with historical test results and dates
        outputs: /
        """
        aki_score, last_test_date = self._get_prediction(patient_data, test_result, historical_tests)
        self.total_predictions.inc()
        # print(f"Aki score: {aki_score}. Timestamp: {last_test_date}")
        if aki_score == 1:
            self.alert_system.trigger_alert(patient_data['mrn'], last_test_date)
            self.paged_list.append(f"{patient_data['mrn']},{last_test_date}")

    def _get_prediction(self, patient_data, test_result, historical_tests):
        """
        Get AKI prediction for a patient using the trained model. Formats the patient data
        to match the input format used to train the ML model.
        inputs:
            - patient_data: dictionary with patient data (MRN, age, sex)
            - test_result: dictionary with the last test result(s) and test date(s)
            - historical_tests: list of dictionaries with historical test results and dates
        outputs:
            - (int): AKI prediction score (0/1)
            - (str): date and time of the last test in YYYY/MM/DD HH:MM:SS format
        """
        # assumes patient data is given as a disctionary
        patient_features = {}
        if type(test_result['test_time']) == list:
            last_test_date = test_result['test_time'][-1]
        else:
            last_test_date = test_result['test_time']
        last_test_result = test_result['test_result']

        creatinine_results = [test['test_result'] for test in historical_tests]
        for test in last_test_result:
            self.blood_test_dist.observe(float(test))
            creatinine_results.append(test)

        patient_features['age'] = patient_data['age']
        patient_features['sex'] = 0 if patient_data['sex']=='m' else 1  # Encode sex as binary
        patient_features['creatinine_mean'] = mean(creatinine_results)
        patient_features['creatinine_median'] = median(creatinine_results)
        patient_features['creatinine_max'] = max(creatinine_results)
        patient_features['creatinine_min'] = min(creatinine_results)
        patient_features['latest_creatinine'] = last_test_result[-1]

        aki_score = self.model.predict([list(patient_features.values())])
        return aki_score, last_test_date

    def diagnostics(self):
        """
        Print system diagnostics (latency, number of sent alerts) and store paged patients.
        Calculates and prints model's F3 score (only if TEST mode enabled).
        inputs: /
        outputs: /
        """
        print("Alert System Diagnostics:")
        print("    Number of Bad (400/500) Requests:", int(self.alert_system.error_alerts._value.get()))
        print("    Number of Retried Pager Alerts:", int(self.alert_system.retried_messages._value.get()))

        print("Prediction System Diagnostics:")
        print("    Total Predictions:", int(self.total_predictions._value.get()))
        print("    Total AKI Detections:", int(self.alert_system._num_pagers._value.get()))
        print("    List of paged patients:", self.paged_list)

        if self.test:
            # save paged patients in a CSV file for system evaluation
            print("    - TEST MODE ENABLED", end='')
            with open('../data/test_aki.csv', 'w') as f:
                f.write("mrn,date\n")
                for paged in self.paged_list:
                    f.write(f"{paged}\n")
            print("        Stored Paged Patients in '../data/test_aki.csv'")
            # print("F3 Score:", self.get_f3_score("../data/aki.csv", "../data/test_aki.csv", self.total_predictions._value.get()))

    def get_f3_score(self, source_filepath, predictions_filepath, total_predictions):
        """
        Get F3 score for the system based on predictions stored in '../data/test_aki.csv' and 
        ground truth cases stored in '../data/aki.csv'.
        Only runs if TEST mode enabled.
        inputs: 
            - source_filepath: path to the ground truth cases file ('../data/aki.csv')
            - predictions_filepath: path to the predictions file ('../data/test_aki.csv')
            - total_predictions: total number of predictions made by the system
        outputs:
            - (float): F3 score for the system
        """
        true_cases = pd.read_csv(source_filepath)
        predicted_cases = pd.read_csv(predictions_filepath)

        # Calculate TP, FN, FP, TN based on the comparison of patient IDs
        TP = len(list(set(true_cases['mrn']) & set(predicted_cases['mrn'])))  # Patients in both true cases and predicted cases (Intersection)
        FN = true_cases[~true_cases['mrn'].isin(predicted_cases['mrn'])].shape[0]  # Patients in true cases but not in predicted cases (Difference)
        FP = predicted_cases[~predicted_cases['mrn'].isin(true_cases['mrn'])].shape[0]  # Patients in predicted cases but not in true cases (Difference)
        TN = total_predictions - TP - FN - FP  # Rest of the patients are true negatives
        # print(f"TP: {TP}, TN: {TN}, FP: {FP}, FN: {FN}")
        f3_score = TP / (TP + 0.1*FP + 0.9*FN)
        return f3_score
        
    