import pandas as pd
from datetime import datetime,timedelta
from collections import Counter,defaultdict
from DataAnalyzer import DataAnalyzer
from UserDataAnalyzer import UserDataAnalyzer
from UserJourneyAnalyzer import UserJourneyAnalyzer
from UserJourneyFinder import UserJourneyFinder,data_directory
from CustomerSegmentation import CustomerSegmentationFactory
from UserBasedCollaborativeFilteringPathPrediction import UserBasedCollaborativeFiltering



#TestCase 1:  For DataAnalyzer and UserJourneyFinder

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






#TestCases for the UserJourneyAnalyzer: 

features_to_use=['userId' or 'distinct_id', 'time' or 'mp_processing_time_ms', 'name' or 'currentScene' or '$name',
                    '$app_release', '$ios_app_release', '$android_app_version', '$ios_app_version']

#Create an instance
data_analyzer = DataAnalyzer(data_directory)
user_journey_finder = UserJourneyFinder(data_directory)

# Process the data
processed_data = data_analyzer.process_data(features_to_use)

# Analyze the data and retrieve the results
result = user_journey_finder.analyze_data(processed_data, features_to_use, year=2022, unit='s')

# Accessing the results
distinct_ids = result['distinct_ids']
user_journeys = result['user_journeys']


# Create an instance of UserJourneyAnalyzer
user_journey_analyzer = UserJourneyAnalyzer(user_journeys)
# Get the results for user journeys
event_name = 1015.675911840711 # Replace 'E' with the specific event
number_of_steps = 1  # Replace with the desired number of consecutive events leading to E
consecutive_event_sets_result = user_journey_analyzer.analyze_user_journeys(event_name=event_name, number_of_steps=number_of_steps)

# Access the results for 'analyze_user_journeys'
consecutive_event_sets = consecutive_event_sets_result['consecutive_event_sets']
common_paths_counter = consecutive_event_sets_result['common_paths_counter']


# Print the sets of consecutive events leading to event E for all users
print(f"Sets of {number_of_steps} consecutive events leading to '{event_name}':")
for user_id, event_sets in consecutive_event_sets.items():
    if event_sets:
        print(f"User ID: {user_id}")
        for event_set in event_sets:
            print(f"Event Set: {event_set}")
        print()
    else:
        print(f"No path leading to event '{event_name}' found for User ID: {user_id}")


# Print the number of common paths leading to event E
print(f"Number of Common Paths leading to event '{event_name}':")
for path, count in common_paths_counter.items():
    print(f"Path: {path}, Count: {count}")





#TestCases for the UserDataAnalyzer: 
features_to_use=['userId' or 'distinct_id', 'time' or 'mp_processing_time_ms', 'name' or 'currentScene' or '$name',
                    '$app_release', '$ios_app_release', '$android_app_version', '$ios_app_version']
# Create an instance
data_analyzer = DataAnalyzer(data_directory)
user_journey_finder = UserJourneyFinder(data_directory)

# Process the data
processed_data = data_analyzer.process_data(features_to_use)

# Analyze the data and retrieve the results
result = user_journey_finder.analyze_data(processed_data, features_to_use, year=2022, unit='s')

# Accessing the results
distinct_ids = result['distinct_ids']
user_journeys = result['user_journeys']

# Create an instance of UserDataAnalyzer
user_data_analyzer = UserDataAnalyzer(user_journeys)

# Get the results for user data
from datetime import datetime, timedelta
time_threshold = timedelta(hours=1)  # Replace with your desired time threshold
user_data_result = user_data_analyzer.analyze_user_data(time_threshold=time_threshold)

# Access the results for 'analyze_user_data'
time_durations = user_data_result['time_durations']
distinct_sessions = user_data_result['distinct_sessions']
average_events_per_session = user_data_result['average_events_per_session']
most_common_time_of_day = user_data_result['most_common_time_of_day']
most_common_day_of_week = user_data_result['most_common_day_of_week']

# # Accessing user journeys for different time periods (optional)
# user_journeys_last_3_months = result['user_journeys_last_3_months']
# user_journeys_three_to_six_months = result['user_journeys_three_to_six_months']
# user_journeys_six_to_twelve_months = result['user_journeys_six_to_twelve_months']

# Print the results for user data
print("Time Durations:", time_durations)

# print("Distinct Sessions:", distinct_sessions)
max_rows_to_print = 10
print("Distinct Sessions:")
for user_id, sessions in distinct_sessions.items():
    print(f"User ID: {user_id}")
    for i, session in enumerate(sessions):
        if i >= max_rows_to_print:
            break
        print(f"Session {i+1}: {session}")
    print("\n")

print("Average Events per Session:", average_events_per_session)
print("Most Common Time of Day:", most_common_time_of_day)
print("Most Common Day of Week:", most_common_day_of_week)


# # Printing user journeys for different time periods (optional)
# print("\nUser Journeys for the Last 3 Months:")
# for user_id, journey in user_journeys_last_3_months.items():
#     print(f"User ID: {user_id}")
#     for step in journey:
#         print(f"App Release: {step['app_release']}, Current Scene: {step['current_scene']}, Timestamp: {step['timestamp']}")
#     print("\n")

# print("\nUser Journeys from 3 to 6 Months Ago:")
# for user_id, journey in user_journeys_three_to_six_months.items():
#     print(f"User ID: {user_id}")
#     for step in journey:
#         print(f"App Release: {step['app_release']}, Current Scene: {step['current_scene']}, Timestamp: {step['timestamp']}")
#     print("\n")

# print("\nUser Journeys from 6 to 12 Months Ago:")
# for user_id, journey in user_journeys_six_to_twelve_months.items():
#     print(f"User ID: {user_id}")
#     for step in journey:
#         print(f"App Release: {step['app_release']}, Current Scene: {step['current_scene']}, Timestamp: {step['timestamp']}")
#     print("\n")






# TestCases for the Customer Segmentation:
factory = CustomerSegmentationFactory()


#features for different types of segmentation to be given
features_for_demographic=['ageGroup', 'gender', '$region', 'profession']
features_for_psychographic=['profession', 'pronoun']

behavioral_segmentation = factory.create_segmentation("Behavioral", user_journeys,[]) #passing empty list because in behavioral we already using the processed data (user_journeys), consisting of features required for this segmenttaion only
behavioral_segmentation.perform_clustering()

demographic_segmentation = factory.create_segmentation("Demographic", 'event_data.csv',features_for_demographic)
demographic_segmentation.perform_clustering(features_for_demographic)

psychographic_segmentation = factory.create_segmentation("Psychographic", 'event_data.csv',features_for_psychographic)
psychographic_segmentation.perform_clustering(features_for_psychographic)


# Example usage for user path prediction using collaborative filtering:

# user_journeys = {user_journeys}  # user_journeys data
collaborative_filtering = UserBasedCollaborativeFiltering(user_journeys)
distinct_ids, similarity_matrix = collaborative_filtering.preprocess_data()

# Predict the next scene for a target user "64"
target_user_id = '64'  # Replace with the desired target user ID
predicted_next_scene = collaborative_filtering.predict_next_scene(int(target_user_id), distinct_ids, similarity_matrix)

print(f"Predicted next scene for {target_user_id}: {predicted_next_scene}")