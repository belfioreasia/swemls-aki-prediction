# ------------------------- MESSAGE MANAGER COMPONENT -------------------------
# Handles incoming MLLP/HL7 messages from the hospital system (simulator).
# Decodes the messages and extracts PAS and LISM data to pass onto the Data Manager.

import logging
from prometheus_client import Counter
from datetime import datetime, date
import hl7

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class MessagesManager():
    """
    Messages Manager component. Handled incoming MLLP messages, decodes them
    and extracts PAS and LISM data to pass onto the Data Manager.
    class parameters:
        - MLLP_START: Start of MLLP message
        - MLLP_END: End of MLLP message
        - MLLP_CR: Carriage return
        - MLLP_BUFFER_SIZE: Buffer Size
        - bad_messages: (counter metric) Number of bad (invalid or corrupted) messages received
    """
    def __init__(self):
        self.MLLP_START = b'\x0b'
        self.MLLP_END = b'\x1c'
        self.MLLP_CR = b'\x0d'
        self.MLLP_BUFFER_SIZE = 1024
        self.bad_messages = Counter('bad_messages', 'Number of bad messages received')

    # PAS Parser (ADT^A01, ADT^A03)
    def parse_pas(self, parsed_hl7, patient_id, message_type):
        """
        Parses PAS messages, either ADT^A01:Admission or ADT^A03:Discharge.
        inputs:
            - parsed_hl7: parsed HL7 message
            - patient_id: patient Medical Record Number (MRN)
            - message_type: HL7 message type (ADT^A01 or ADT^A03)
        outputs:
            - (int): patient Medical Record Number (MRN)
            - (str): event type ('admitted' or 'discharged')
            - (dict): patient data (name, age, sex)
        """
        if message_type == "ADT^A01":
            patient_name, age, sex = None, None, None
            for segment in parsed_hl7:
                if str(segment[0]) == "PID":
                    patient_name = segment[5][0] if len(segment[5]) > 0 else None
                    dob = str(segment[7][0]) if len(segment[7]) > 0 else None

                    # Convert DOB to Age
                    if dob:
                        dob = datetime.strptime(dob, "%Y%m%d").date()
                        today = date.today()
                        age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))

                    sex = segment[8][0] if len(segment[8]) > 0 else None
            
            return patient_id, "admitted", {"name": patient_name, "age": age, "sex": sex}

        if message_type == "ADT^A03":
            return patient_id, "discharged", None  # No extra data needed

        return patient_id, "unknown", {}

    # LIMS Parser (ORU^R01)
    def parse_lims(self, parsed_hl7, patient_id):
        """
        Parses LIMS messages (ORU^R01 - Lab Results).
        inputs:
            - parsed_hl7: parsed HL7 message
            - patient_id: patient Medical Record Number (MRN)
        outputs:
            - (int): patient Medical Record Number (MRN)
            - (str): event type ('admitted' or 'discharged')
            - (dict): patient data (test results and dates)
        """
        test_results = []
        test_time = None

        for segment in parsed_hl7:
            if str(segment[0]) == "OBR":
                if len(segment) > 7:  # Extract Test Timestamp
                    test_time = datetime.strptime(str(segment[7]), "%Y%m%d%H%M%S")
                else:
                    test_time = datetime.now()
                test_time = test_time.strftime("%Y-%m-%d %H:%M:%S")

            if str(segment[0]) == "OBX" and len(segment) > 5:  # Extract Test Results
                test_type = str(segment[3])
                test_value = str(segment[5])
                test_results.append(float(test_value))
                # print(f"Test Type: {test_type}, Value: {test_value}")
        return patient_id, "test_result", {"tests": test_results, "test_time": test_time}

    # MLLP to HL7 Message Decoder
    def decode_hl7_message(self, hl7_message):
        """
        Decodes HL7 messages and extracts message type (PAS or LISM) and patient ID.
        inputs:
            - hl7_message: received MLLP message
        outputs:
            - (str): parsed HL7 message
            - (int): patient Medical Record Number (MRN)
            - (dict): message type 
                      - for PAS: ADT^A01 or ADT^A03
                      - for LISM: ORU^R01
        """
        try:
            parsed_hl7 = hl7.parse(hl7_message, encoding="latin1")

            # Extract message type (MSH.9)
            message_type = str(parsed_hl7.segment("MSH")[9]) if len(parsed_hl7.segment("MSH")) > 9 else "UNKNOWN"
            # print(f"Message Type: {message_type}", end='')

            # Extract Patient ID (MRN) from PID.3
            patient_id = None
            for segment in parsed_hl7:
                if str(segment[0]) == "PID":  # Look for PID segment
                    patient_id = int(str(segment[3])) if len(segment) > 3 else None  # MRN

            if not patient_id:
                logging.error("Error: No patient ID found in HL7 message.")
                return None, "error", {}

            return parsed_hl7, patient_id, message_type

        except Exception as e:
            logging.error(f"Error decoding HL7 message.")
            print(e)
            self.bad_messages.inc()
            return None, None, "error"

    # Unified Parser Function
    def parse_hl7(self, hl7_message):
        """
        Decides message type and handles it accordingly based on whether the message 
        is a PAS or LIMS.
        inputs:
            - hl7_message: received MLLP message
        outputs:
            - (int): patient Medical Record Number (MRN)
            - (str): event type ('admitted','discharged' or 'test_result')
            - (dict): patient data
                      - for PAS: name, age, sex
                      - for LISM: test results and dates
        """
        parsed_hl7, patient_id, message_type = self.decode_hl7_message(hl7_message)
        if parsed_hl7 is None:
            return None, "error", {}

        # Route to the correct parser
        if message_type.startswith("ADT"):  # PAS Message
            logging.info(f"Received PAS for patient {patient_id}")
            return self.parse_pas(parsed_hl7, patient_id, message_type)
        
        if message_type == "ORU^R01":  # LIMS Message
            logging.info(f"Received LISM for patient {patient_id}")
            return self.parse_lims(parsed_hl7, patient_id)

        logging.error(f"Unknown message. Ignoring.")
        return patient_id, "unknown", {}

    def handle_message(self, buffer):
        """
        Handles an incoming HL7 message, decodes it and extracts data.
        inputs:
            - buffer: received message
        outputs:
            - (int): patient Medical Record Number (MRN)
            - (str): event type ('admitted','discharged' or 'test_result')
            - (dict): patient data
                      - for PAS: name, age, sex
                      - for LISM: test results and dates
        """
        try:            
            # Decode the HL7 message
            message = buffer.strip(self.MLLP_START + self.MLLP_END + self.MLLP_CR).decode("utf-8")
            # Parse the HL7 message
            patient_id, event, message = self.parse_hl7(message)
            
            return patient_id, event, message

        except Exception as e:
            # print(f"Error handling HL7 message: {e}")
            logging.error(f"Error handling HL7 message.")
            print(e)
            self.bad_messages.inc()
            return None, "error", {}

    def diagnostics(self):
        """
        Returns the number of bad messages received.
        """
        print(f" (of which {int(self.bad_messages._value.get())} were invalid)")