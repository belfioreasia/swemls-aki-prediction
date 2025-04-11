import argparse
import csv
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import joblib

# Train the model using the provided training dataset
def train_model(training_data_path):

    training_data = pd.read_csv(training_data_path)

    # Preprocessing : 

    # Identify columns containing creatinine results and dates to perform feature aggregation
    creatinine_result_columns = [col for col in training_data.columns if 'creatinine_result' in col]
    creatinine_date_columns = [col for col in training_data.columns if 'creatinine_date' in col]

    # Aggregate the creatinine results for each patient
    training_data['creatinine_mean'] = training_data[creatinine_result_columns].mean(axis=1)
    training_data['creatinine_median'] = training_data[creatinine_result_columns].median(axis=1)
    training_data['creatinine_max'] = training_data[creatinine_result_columns].max(axis=1)
    training_data['creatinine_min'] = training_data[creatinine_result_columns].min(axis=1)

    # Extract the latest non-null test result for each patient as it can serve as a critical feature
    training_data['latest_creatinine'] = training_data[creatinine_result_columns].apply(lambda row: row.dropna().iloc[-1], axis=1)

    # Drop the original creatinine result and date columns as they are no longer needed after aggregation
    training_data = training_data.drop(columns=creatinine_result_columns + creatinine_date_columns)

    # Encode 'sex' as a binary variable (0 for male, 1 for female)
    training_data['sex'] = training_data['sex'].map({'m': 0, 'f': 1})  # Encode sex as binary

    # Define features (X) and target variable (y) for training
    X = training_data[['age', 'sex', 'creatinine_mean', 'creatinine_median', 'creatinine_max', 'creatinine_min', 'latest_creatinine']]
    y = training_data['aki'].map({'n': 0, 'y': 1})  # Encode 'aki' as binary

    # Train a Random Forest Classifier. n_estimators=100 ensures sufficient trees for good predictions while balancing computational efficiency
    clf = RandomForestClassifier(n_estimators=100)  
    clf.fit(X.values, y)

    return clf

# Predict AKI using the trained model and save predictions to a file
def predict(input_path, output_path, model):

    clf = model
    test_data = pd.read_csv(input_path)

    # Preprocessing: Perform the same feature aggregation as during training
    creatinine_result_columns = [col for col in test_data.columns if 'creatinine_result' in col]
    creatinine_date_columns = [col for col in test_data.columns if 'creatinine_date' in col]

    test_data['creatinine_mean'] = test_data[creatinine_result_columns].mean(axis=1)
    test_data['creatinine_median'] = test_data[creatinine_result_columns].median(axis=1)
    test_data['creatinine_max'] = test_data[creatinine_result_columns].max(axis=1)
    test_data['creatinine_min'] = test_data[creatinine_result_columns].min(axis=1)

    test_data['latest_creatinine'] = test_data[creatinine_result_columns].apply(lambda row: row.dropna().iloc[-1], axis=1)
    test_data = test_data.drop(columns=creatinine_result_columns + creatinine_date_columns)
    test_data['sex'] = test_data['sex'].map({'m': 0, 'f': 1})  # Encode sex as binary

    X_test = test_data[['age', 'sex', 'creatinine_mean', 'creatinine_median', 'creatinine_max', 'creatinine_min', 'latest_creatinine']]

    predictions = clf.predict(X_test)

    # Convert predictions back to 'y' and 'n'
    aki_predictions = ['y' if pred == 1 else 'n' for pred in predictions]

    with open(output_path, 'w', newline='') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(['aki'])
        writer.writerows([[pred] for pred in aki_predictions])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="training_data/test.csv", help="Input CSV file")
    parser.add_argument("--output", default="training_data/aki.csv", help="Output CSV file")
    parser.add_argument("--training_data", default="training_data/training.csv", help="Training data file")
    flags = parser.parse_args()

    model = train_model(flags.training_data)
    joblib.dump(model, '../model.pt')

    predict(flags.input, flags.output, model)

if __name__ == "__main__": 
    main()
    