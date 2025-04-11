# ------------------------- MAIN -------------------------
# The skeleton of the main system that connects all components.
# It ensures that the system can receive HL7 messages (checks for socket connection) and
# sends them to the 'MessagesMAnager' component for processing.
# It redirects processed data to the 'DataManager' component for storage and handling of prediction queue.
# It then sends patient data ready for prediction to the 'PredictionSystem' component,
# which runs the AKI prediction algorithm and pages clinitians (if necessary).
# It finally sends acknowledgments after each message is received and processed.

import socket
import time
import os
import logging
import signal
from prometheus_client import Counter, Gauge, start_http_server
from messages_manager import MessagesManager
from data_manager import DataManager
from prediction_system import PredictionSystem
from custom_errors import SigtermException, SigintException


# Global Connection Info
MLLP_ADDRESS = os.getenv('MLLP_ADDRESS', "host.docker.internal:8440")
PAGER_ADDRESS = os.getenv('PAGER_ADDRESS', "host.docker.internal:8441")

# MLLP Protocol Constants
MLLP_START = b'\x0b'
MLLP_END = b'\x1c'
MLLP_CR = b'\x0d'
MLLP_BUFFER_SIZE = 1024

# Database Info
DB_PATH = os.getenv('DB_PATH', "../data/hospital_aki.db") # persistent storage
DB_FILLED = False

# Connection Handling Defaults
ALLOWED_MLLP_TIMEOUT_SECONDS = 10
MLLP_CONNECTION_DELAY = 1
TERMINATE_CONNECTION = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def connect_to_mllp(sock):
    """
    Establish connection with the Hospital's's MLLP server.
    inputs: 
        - (socket): socket object connected to the MLLP server
    outputs: /
    """
    global MLLP_CONNECTION_DELAY
    if connection_tries._value.get() > 1:
        if connection_tries._value.get() == 2:
            print(f"\nWaiting for MLLP server...")
        time.sleep(MLLP_CONNECTION_DELAY) 
    else:
        print("\nConnecting to MLLP server...")
    full_address = MLLP_ADDRESS.split(':')
    HOST, MLLP_PORT = full_address[0], int(full_address[1])
    MLLP_CONNECTION_DELAY = min(MLLP_CONNECTION_DELAY * 2, 5) # exponential backoff with max 5s delay
    sock.connect((HOST, MLLP_PORT))
    
    logging.info(f"Established connection to MLLP server at {HOST}:{MLLP_PORT}")
    

def send_ack(sock, accepted=True):
    """
    Send an MLLP acknowledgment (AA) to the hospital's system.
    Handles connection errors.
    inputs: 
        - sock: socket object connected to the MLLP server
    outputs:
        - (bool): True if the message was sent successfully, False otherwise
    """
    try:
        if accepted:
            ack_message = b"MSH|^~\\&|||||20240129093837||ACK|||2.5\rMSA|AA\r"
        else:
            ack_message = b"MSH|^~\\&|||||20240129093837||ACK|||2.5\rMSA|AE\r"
        
        sock.sendall(MLLP_START + ack_message + MLLP_END + MLLP_CR)
        return True
    except (ConnectionError, TimeoutError):
        return False


def handle_sigterm():
    """ 
    Custiom Handling of SIGTERM signal (graceful shutdown)
    """
    logging.info(f"Received SIGTERM. Graceful shutdown...")
    raise SigtermException()


def handle_sigint():
    """ 
    Custiom Handling of SIGINT signal (graceful shutdown)
    """
    logging.info(f"Received SIGINT. Graceful shutdown...")
    raise SigintException() 
    

def run_system(sock, messages_manager, data_manager, prediction_system, predictions, received_messages):
    """
    Run the system for one full connection cycle. It keeps the connection open and listens for incoming messages
    until the connection is interrupted by the hospital system, by the user, due to timeout or any other error.
    inputs: 
        - sock: socket object connected to the MLLP server
        - messages_manager: instance of the MessagesManager class
        - data_manager: instance of the DataManager class
        - prediction_system: instance of the PredictionSystem class
        - predictions: Counter object to count the number of predictions
        - received_messages: Counter object to count the number of received messages
    outputs
        - /
    """
    global TERMINATE_CONNECTION
    global MLLP_CONNECTION_DELAY
    global connection_tries

    server_open = True
    connection_tries.set(1)
    MLLP_CONNECTION_DELAY = 0.5 # reset connection delay

    try:
        while server_open:
            buffer = b''
            buffer_done = False
            while not buffer_done:
                chunk = sock.recv(MLLP_BUFFER_SIZE)
                if (not chunk) or (chunk==b''):  # Connection closed by server
                    server_open = False
                    raise ConnectionError("Connection closed by server.")
                buffer += chunk
                if buffer.endswith(MLLP_END + MLLP_CR):
                    buffer_done = True
                
            received_messages.inc()

            # Step 1: Parse HL7 Message
            patient_id, event, message = messages_manager.handle_message(buffer)
            if patient_id is not None:
                message_accept=True
                # Step 2: Handle Patient Data (Store & Queue)
                ready_patients = data_manager.handle_patient_data(patient_id, event, message)
                # Step 3: AKI Prediction
                for (patient_data, test_result, historical_tests) in ready_patients:
                    prediction_system.run(patient_data, test_result, historical_tests)
                    predictions.inc()
                    data_manager.remove_from_ready_queue(patient_data, test_result, historical_tests)
            else:
                message_accept=False

            # Step 4: Send Message Acknowledgement
            if not send_ack(sock, message_accept):
                logging.error("Error sending acknowledgment.")
                raise ConnectionError("Error sending acknowledgment.") from None
        
    except ConnectionError:
        print("\nConnection interrupted by peer.")

    except (SigtermException, SigintException, KeyboardInterrupt): # handle signal interruption
        print("\nSignal connection interruption.")
        TERMINATE_CONNECTION = True


def main():
    """
    Main function that connects all system components.
    global parameters:
        - HOST = "host.docker.internal" : to connect to system
        - MLLP_ADDRESS = 8440 : to connect to MLLP port (incoming)
        - PAGER_ADDRESS = 8441 : to connect to Pager port (outgoing)
        - MLLP_START: MLLP message start byte
        - MLLP_END: MLLP message end byte
        - MLLP_CR: MLLP carriage return
        - MLLP_BUFFER_SIZE: size of buffer to receive messages
        - DB_PATH: path and name of the database file
        - DB_FILLED: flag to indicate if the database should be created from scratch or pre-loaded
        - ALLOWED_MLLP_TIMEOUT_SECONDS: maximum allowed time to wait for MLLP connection
        - MLLP_CONNECTION_DELAY: delay between connection retries
        - ALLOWED_RETRIES: number of allowed connection retries
        - TERMINATE_CONNECTION: flag to indicate if connection should be terminated (graceful shutdown)
        - connection_tries: Prometheus Gauge to keep track of connection retries
    main functions:
        - run_system(): run the system for one full connection cycle
        - connect_to_mllp(): ensure connection to MLLP simulator
        - send_ack(): send message acknowledgment after message is processed
        - main(): main function that connects all system components (runs the system)
    """
    global TERMINATE_CONNECTION
    global connection_tries

    logging.info("Started System")
    start_runtime = time.time()

    predictions = Counter("predictions_num","Number of total model predictions queried")
    received_messages = Counter("messaged_received","Number of messages received")
    total_mllp_connections = Counter("total_reconnections","Number of times the system reconnected to the MLLP server")

    # Initialize System Components
    try:
        messages_manager = MessagesManager()
        data_manager = DataManager(DB_FILLED=DB_FILLED)
        prediction_system = PredictionSystem(test=False)
    except Exception as e:
        logging.error(f"Error initializing system components: {e}")
        logging.info("System Shutdown.")
        return

    while not TERMINATE_CONNECTION:
        try:
            if connection_tries._value.get() > 1:
                time.sleep(1) # wait for 1 second before retrying connection
            
            connection_tries.inc()
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                # connect to MLLP server
                sock.settimeout(ALLOWED_MLLP_TIMEOUT_SECONDS)
                connect_to_mllp(sock)
                total_mllp_connections.inc()

                # run main loop: continues until connection is interrupted by the hospital server
                run_system(sock, messages_manager, data_manager, prediction_system, predictions, received_messages)
        
        except TimeoutError:
            if connection_tries._value.get() <= 1:
                print("\nMLLP Connection timeout.")

        except (SigtermException, SigintException, KeyboardInterrupt): # handle signal interruption
            print("\nSignal connection interruption.")
            TERMINATE_CONNECTION = True
        
        except Exception as e: # handle any other errors
            if connection_tries._value.get() <= 1:
                logging.error(f"{e}")


    logging.info(f"Closing connection after {int(total_mllp_connections._value.get())} retries.")
    runtime = time.time() - start_runtime

    print("\nPrinting System Diagnostics:")
    print(f"    Total System Runtime: {runtime:.2f} seconds")
    if received_messages._value.get() > 1:
        print(f"    Total Messages Received: {int(received_messages._value.get())}", end='') 
        messages_manager.diagnostics()
        prediction_system.diagnostics()
        data_manager.diagnostics()
    print()
    
    time.sleep(0.5)  # wait before closing system
    # shutdown system
    data_manager.close_connection()
    sock.close()
    logging.info("System Shutdown.")


if __name__ == "__main__":

    # load historical data on database only when starting the system for the first time
    if os.path.exists(DB_PATH):
        DB_FILLED = True
        print(f"Database pre-loaded at {DB_PATH}.")
    else:
        print(os.path.dirname(DB_PATH))
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    print("Pager Port: ", PAGER_ADDRESS)
    print("MLLP Port: ", MLLP_ADDRESS)

    start_http_server(8000)
    print("Prometheus Metrics Port: 0.0.0.0:8000\n")

    signal.signal(signal.SIGTERM, lambda signal, frame: handle_sigterm()) # handle sigterm
    signal.signal(signal.SIGINT, lambda signal, frame: handle_sigint()) # handle sigint

    global connection_tries
    connection_tries = Gauge("connection_tries","Number of times the system tried to connect to the MLLP server (Resets on successful connection)")
    connection_tries.set(0)
    
    main()