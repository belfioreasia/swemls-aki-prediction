import unittest
import hl7
from system.messages_manager import MessagesManager

class TestMessagesManager(unittest.TestCase):

    def setUp(self):
        """Initialize a fresh MessagesManager instance for each test."""
        self.manager = MessagesManager()

    ## ------------------------ TEST: decode_hl7_message ------------------------ ##
    
    def test_decode_valid_hl7_message(self):
        """Test decoding a well-formed HL7 message with patient ID."""
        hl7_message = (
            "MSH|^~\\&|SIMULATION|SOUTH RIVERSIDE|||20240203143300||ADT^A01|||2.5\r"
            "PID|1||173305613||HAWWA HOOPER||19980114|F"
        )

        parsed_hl7, patient_id, message_type = self.manager.decode_hl7_message(hl7_message)

        self.assertIsNotNone(parsed_hl7)
        self.assertEqual(patient_id, 173305613)
        self.assertEqual(message_type, "ADT^A01")

    def test_decode_hl7_message_missing_patient_id(self):
        """Test decoding an HL7 message without a patient ID."""
        hl7_message = (
            "MSH|^~\\&|SIMULATION|SOUTH RIVERSIDE|||20240203143300||ADT^A01|||2.5\r"
            "PID|1||||HAWWA HOOPER||19980114|F"
        )

        parsed_hl7, patient_id, message_type = self.manager.decode_hl7_message(hl7_message)

        self.assertIsNone(patient_id)
        self.assertEqual(message_type, "error")

    def test_decode_invalid_hl7_message(self):
        """Test handling of a completely invalid HL7 message."""
        hl7_message = "INVALID MESSAGE FORMAT"

        parsed_hl7, patient_id, message_type = self.manager.decode_hl7_message(hl7_message)
        
        self.assertIsNone(parsed_hl7)
        self.assertIsNone(patient_id)
        self.assertEqual(message_type, "error")

    ## ------------------------ TEST: parse_pas ------------------------ ##
    
    def test_parse_admission_pas_message(self):
        """Test parsing a PAS admission message."""
        hl7_message = (
            "MSH|^~\\&|SIMULATION|SOUTH RIVERSIDE|||20240203143300||ADT^A01|||2.5\r"
            "PID|1||173305613||HAWWA HOOPER||19980114|F"
        )

        parsed_hl7, patient_id, message_type = self.manager.decode_hl7_message(hl7_message)
        patient_id, event, data = self.manager.parse_pas(parsed_hl7, patient_id, message_type)

        self.assertEqual(patient_id, 173305613)
        self.assertEqual(event, "admitted")
        self.assertEqual(data["name"], "HAWWA HOOPER")
        self.assertEqual(data["age"], 27)  # Assuming current year is 2025, before birthday
        self.assertEqual(data["sex"], "F")

    def test_parse_discharge_pas_message(self):
        """Test parsing a PAS discharge message."""
        hl7_message = (
            "MSH|^~\\&|SIMULATION|SOUTH RIVERSIDE|||20240203143300||ADT^A03|||2.5\r"
            "PID|1||173305613||HAWWA HOOPER||19980114|F"
        )

        parsed_hl7, patient_id, message_type = self.manager.decode_hl7_message(hl7_message)
        patient_id, event, data = self.manager.parse_pas(parsed_hl7, patient_id, message_type)

        self.assertEqual(patient_id, 173305613)
        self.assertEqual(event, "discharged")
        self.assertIsNone(data)

    ## ------------------------ TEST: parse_lims ------------------------ ##
    
    def test_parse_lims_message(self):
        """Test parsing a LIMS lab result message."""
        hl7_message = (
            "MSH|^~\\&|SIMULATION|SOUTH RIVERSIDE|||20241227170400||ORU^R01|||2.5\r"
            "PID|1||197034342\r"
            "OBR|1||||||20241227170400\r"
            "OBX|1|SN|CREATININE||133.3700653106997"
        )

        parsed_hl7, patient_id, message_type = self.manager.decode_hl7_message(hl7_message)
        patient_id, event, data = self.manager.parse_lims(parsed_hl7, patient_id)

        self.assertEqual(patient_id, 197034342)
        self.assertEqual(event, "test_result")
        self.assertEqual(data["tests"], [133.3700653106997])
        self.assertEqual(data["test_time"], "2024-12-27 17:04:00")

    ## ------------------------ TEST: parse_hl7 ------------------------ ##
    
    def test_parse_hl7_pas_message(self):
        """Test full HL7 PAS parsing flow."""
        hl7_message = (
            "MSH|^~\\&|SIMULATION|SOUTH RIVERSIDE|||20241227171400||ADT^A01|||2.5\r"
            "PID|1||147795943||SIYANA WILLIAMSON||20110727|F"
        )

        patient_id, event, data = self.manager.parse_hl7(hl7_message)

        self.assertEqual(patient_id, 147795943)
        self.assertEqual(event, "admitted")
        self.assertEqual(data["name"], "SIYANA WILLIAMSON")

    def test_parse_hl7_invalid_message(self):
        """Test handling of an invalid HL7 message."""
        hl7_message = "INVALID MESSAGE FORMAT"

        patient_id, event, data = self.manager.parse_hl7(hl7_message)

        self.assertIsNone(patient_id)
        self.assertEqual(event, "error")
        self.assertEqual(data, {})

if __name__ == "__main__":
    unittest.main()