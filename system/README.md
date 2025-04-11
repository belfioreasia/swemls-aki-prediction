# AKI Prediction System

The full system design specifications can be found [here](system/system_design).

![Prediction System Design](/system/system_design/system-design.png)


## Overview

The [main](system/main.py) file connects all system components and represents the general system pipeline. \
It establishes the connection with the Hospital's MLLP system and sends acknowledgment (ACK) messages after every incoming message is fully processed.


## Components

- [Messages Manager Component](system/messages_manager.py): Handles incoming MLLP/HL7 messages from the hospital system (simulator), decodes them into HL7 and extracts PAS and LISM data to pass onto the Data Manager.

- [Data Manager Component](system/data_manager.py): Handles decoded data received from the Messages Manager Component and keeps track (through a queue) of the order in which patient data (LISM) comes in. Updates the databases with the received data (ensuring new patients are correctly added, admission/discharge status is changed for present patients, new blood tests are added). Finally sends patient data to the Prediction System.

- [Database System Component](system/database_system.py): Internal SQL Database with patient data (patient MRN, age, sex, admission status) and blood test results (patient MRN, test date and time and test result). Connected to the system through the Data Manager Component.

- [Prediction System Component](system/prediction_system.py): Receives patient and test data from the 'Data Manager' component, formats it and uses the pre-trained ML model to predict aki status. It triggers the 'Alert System' sub-component to page the clinitians if AKI is detected. It ensures alerts are correctly received and re-pages the clinitians up to two times if alerts are unsuccessful.
If TEST mode is enabled, it tracks maximum alert latency and stores the list of paged patients (MRN, test date) in a [CSV file](data/test_aki.csv) for performance evaluation.