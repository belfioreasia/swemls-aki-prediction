import unittest
from unittest.mock import patch, MagicMock
from system.prediction_system import PredictionSystem, AlertSystem

class TestAlertSystem(unittest.TestCase):
    """Unit tests for the AlertSystem component."""

    def setUp(self):
        """Set up AlertSystem instance."""
        self.alert_system = AlertSystem()

    def test_check_post_health(self):
        """Test the _check_post_health method."""
        self.assertEqual(self.alert_system._check_post_health(200), "OK")
        self.assertEqual(self.alert_system._check_post_health(400), "IGNORE")
        self.assertEqual(self.alert_system._check_post_health(500), "RETRY")
        self.assertEqual(self.alert_system._check_post_health(404), "IGNORE")

    @patch("system.prediction_system.requests.post")
    def test_post(self, mock_post):
        """Test that _post sends correct request and handles responses."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_post.return_value = mock_response

        status = self.alert_system._post("123456", "20240205120000")
        self.assertEqual(status, "OK")
        mock_post.assert_called_once_with(
            self.alert_system._pager_url,
            data="123456,20240205120000",
            headers={'Content-Type': 'text/plain'}
        )

    @patch.object(AlertSystem, "_post", return_value="OK")
    def test_trigger_alert_success(self, mock_post):
        """Test that trigger_alert increases _num_pagers when successful."""
        initial_pagers = self.alert_system._num_pagers
        self.alert_system.trigger_alert(123456, "2024-02-05 12:00:00")
        self.assertEqual(self.alert_system._num_pagers, initial_pagers + 1)


class TestPredictionSystem(unittest.TestCase):
    """Unit tests for the PredictionSystem component."""

    def setUp(self):
        """Set up PredictionSystem instance."""
        self.prediction_system = PredictionSystem(test=True)
        self.prediction_system.model = MagicMock()  # Mock model

    def test_get_prediction(self):
        """Test _get_prediction processes input correctly."""
        patient_data = {"mrn":123456, "age": 50, "sex": "m"}
        test_result = {"test_time": "2024-02-05 12:00:00", "test_result": [180]}
        historical_tests = [{"test_result": 100, "test_time": "2024-02-01 12:00:00"}]

        self.prediction_system.model.predict.return_value = 1  # Simulate positive prediction

        aki_score, last_test_date = self.prediction_system._get_prediction(patient_data, test_result, historical_tests)
        
        self.assertEqual(aki_score, 1)
        self.assertEqual(last_test_date, "2024-02-05 12:00:00")

    @patch.object(PredictionSystem, "_get_prediction", return_value=(1, "2024-02-05 12:00:00"))
    @patch.object(AlertSystem, "trigger_alert")
    def test_run_alert_triggered(self, mock_alert, mock_prediction):
        """Test that run() triggers alert when AKI is detected."""
        patient_data = {"mrn":123456, "age": 50, "sex": "m"}
        test_result = {"test_time": "2024-02-05 12:00:00", "test_result": [180]}
        historical_tests = [{"test_result": 100, "test_time": "2024-02-01 12:00:00"}]

        self.prediction_system.run(patient_data, test_result, historical_tests)

        mock_prediction.assert_called_once()
        mock_alert.assert_called_once_with(123456, "2024-02-05 12:00:00")
"""
    @patch("builtins.open", new_callable=MagicMock)
    def test_diagnostics(self, mock_open):
        #Test diagnostics output and file writing in test mode.
        self.prediction_system.paged_list = ["123456,2024-02-05 12:00:00"]
        self.prediction_system.diagnostics(1)
        mock_open.assert_called_once_with('data/test_aki.csv', 'w')
"""

"""
class IntegrationTestPredictionSystem(unittest.TestCase):
    #Integration tests for the full Prediction System.

    @patch.object(AlertSystem, "trigger_alert")
    def test_prediction_and_alert(self, mock_alert):
        #Test end-to-end prediction and alert triggering.
        prediction_system = PredictionSystem(test=True)
        prediction_system.model = MagicMock()
        prediction_system.model.predict.return_value = [1]  # AKI detected

        patient_data = {"mrn":123456, "age": 50, "sex": "m"}
        test_result = {"test_time": "2024-02-05 12:00:00", "test_result": [180]}
        historical_tests = [{"test_result": 100, "test_time": "2024-02-01 12:00:00"}]

        prediction_system.run(patient_data, test_result, historical_tests)

        mock_alert.assert_called_once_with(789012, "2024-02-05 08:30:00")
"""

if __name__ == '__main__':
    unittest.main()
