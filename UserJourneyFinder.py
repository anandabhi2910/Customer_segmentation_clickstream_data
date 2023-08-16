import pandas as pd
from datetime import datetime,timedelta
import csv
from collections import Counter,defaultdict
import os
import configparser
import DataAnalyzer

#create a config file with the relative paths and environment variables
config_file_path = '/content/config.ini'

# Example: Writing the config file using Python
config_content = """
[Paths]
data_directory = /content/drive/MyDrive/event_data
output_directory = /content/combined_csv_files

[EnvironmentVariables]
DATA_DIR = ${Paths:data_directory}
OUTPUT_DIR = ${Paths:output_directory}
"""

with open(config_file_path, 'w') as config_file:
    config_file.write(config_content)


# Read the config file
config = configparser.ConfigParser()
config.read(config_file_path)

# Get the paths from the config file
data_directory = config['Paths']['data_directory']
output_directory = config['Paths']['output_directory']

# Set the environment variables
os.environ['DATA_DIR'] = data_directory
os.environ['OUTPUT_DIR'] = output_directory

class UserJourneyFinder:
    def __init__(self, data_directory):
        self.data_analyzer = DataAnalyzer(data_directory)

    def analyze_data(self, processed_data, features_to_use, year, unit):
        try:
            # Load the Data
            columns_to_use = features_to_use
            self.data_analyzer.data = processed_data

            # Check for Null Values
            null_values = self.data_analyzer.data.isnull().sum()

            # Convert Unix to Datetime (if year and unit are provided)
            if year and unit:
                datetime_column = 'time'
                new_col = 'datetime_format'
                if unit == 's':
                    self.data_analyzer.data[new_col] = pd.to_datetime(self.data_analyzer.data[datetime_column], unit='s')
                elif unit == 'ms':
                    self.data_analyzer.data[new_col] = pd.to_datetime(self.data_analyzer.data[datetime_column], unit='ms')
                else:
                    print(f"Error: Invalid unit '{unit}'. Supported units are 's' and 'ms'.")

                # Extract Month, Day, Year
                self.data_analyzer.data['Month'] = self.data_analyzer.data[new_col].dt.month
                self.data_analyzer.data['Day'] = self.data_analyzer.data[new_col].dt.day
                self.data_analyzer.data['Year'] = self.data_analyzer.data[new_col].dt.year

                # Filter Data by Year
                if year:
                    self.data_analyzer.data = self.data_analyzer.data[(self.data_analyzer.data['Month'].between(1, 12)) & (self.data_analyzer.data['Year'] == year)]
                self.data_analyzer.data.dropna(subset=['modified_distinct_id', 'Month', 'Day', 'Year'], inplace=True)

            # Save to CSV (if needed)
            # self.save_to_csv(output_file)

            # Extract Distinct IDs (if needed)
            distinct_ids = self.data_analyzer.extract_distinct_ids()

            # Create User Journeys
            user_journeys = self.find_user_journeys(year, unit)

            return {
                'data': self.data_analyzer.data,
                'null_values': null_values,
                'summary_statistics': self.data_analyzer.data.describe(),
                'distinct_ids': distinct_ids,
                'user_journeys': user_journeys
            }
        except FileNotFoundError:
            print("Error: File not found.")
        except Exception as e:
            print(f"Error: An error occurred. {str(e)}")

    

    def find_user_journeys(self, year, unit):
          user_journeys = {}
          try:
              if unit == 's':
                  self.data_analyzer.data['datetime_format'] = pd.to_datetime(self.data_analyzer.data['time'], unit='s')
              elif unit == 'ms':
                  self.data_analyzer.data['datetime_format'] = pd.to_datetime(self.data_analyzer.data['time'], unit='s')
              else:
                  print(f"Error: Invalid unit '{unit}'. Supported units are 's' and 'ms'.")

              self.data_analyzer.data['Month'] = self.data_analyzer.data['datetime_format'].dt.month
              self.data_analyzer.data['Day'] = self.data_analyzer.data['datetime_format'].dt.day
              self.data_analyzer.data['Year'] = self.data_analyzer.data['datetime_format'].dt.year

              filtered_chunk = self.data_analyzer.data[(self.data_analyzer.data['Month'].between(1, 12)) & (self.data_analyzer.data['Year'] == year)]
              filtered_chunk.dropna(subset=['modified_distinct_id', 'Month', 'Day', 'Year'], inplace=True)

              for _, row in filtered_chunk.iterrows():
                  distinct_id = row['modified_distinct_id']
                  app_release = row['$app_release'] or row['$ios_app_release'] or row['$android_app_version']
                  current_scene = row['name']
                  timestamp = row['datetime_format']

                  if distinct_id not in user_journeys:
                      user_journeys[distinct_id] = []

                  user_journeys[distinct_id].append({
                      'app_release': app_release,
                      'current_scene': current_scene,
                      'timestamp': timestamp
                  })
          except Exception as e:
              print(f"Error: An error occurred. {str(e)}")

          return user_journeys


if __name__ =="__main__":
    # Example usage:
    features_to_use=['userId' or 'distinct_id', 'time' or 'mp_processing_time_ms', 'name' or 'currentScene' or '$name',
                    '$app_release', '$ios_app_release', '$android_app_version', '$ios_app_version']
    # data_directory = '/content/event_data'  # Don't uncomment this, we are using the environment variables

    # Create instances of the classes
    data_analyzer = DataAnalyzer(data_directory)
    user_journey_finder = UserJourneyFinder(data_directory)

    # Process the data
    processed_data = data_analyzer.process_data(features_to_use)

    # Analyze the data and retrieve the results
    result = user_journey_finder.analyze_data(processed_data, features_to_use, year=2022, unit='s')

    # Accessing the results
    distinct_ids = result['distinct_ids']
    user_journeys = result['user_journeys']

    # Accessing user journeys and distinct IDs
    print("Distinct IDs:")
    print(distinct_ids)
    
    print("\nUser Journeys:")
    for user_id, journey in user_journeys.items():
        print(f"User ID: {user_id}")
        for step in journey:
            print(f"App Release: {step['app_release']}, Current Scene: {step['current_scene']}, Timestamp: {step['timestamp']}")
        print("\n")


