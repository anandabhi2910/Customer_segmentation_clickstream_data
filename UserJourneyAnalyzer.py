from collections import defaultdict, Counter
from datetime import datetime, timedelta
from DataAnalyzer import DataAnalyzer
from UserJourneyFinder import UserJourneyFinder
from UserJourneyFinder import data_directory

class UserJourneyAnalyzer:
    def __init__(self, user_journeys):
        self.user_journeys = user_journeys

    def analyze_user_journeys(self, event_name, number_of_steps):
        consecutive_event_sets = defaultdict(list)
        common_paths_counter = Counter()

        for user_id, journey in self.user_journeys.items():
            try:
                events = [event['current_scene'] for event in journey]
                if event_name in events:
                    event_index = events.index(event_name)
                    if event_index >= number_of_steps:
                        consecutive_events = events[event_index - number_of_steps:event_index]
                        consecutive_event_sets[user_id].append(consecutive_events)
                    else:
                        print(f"Number of steps: {number_of_steps} doesn't lead to event {event_name} for User ID: {user_id}. Try a different number of steps.")
                else:
                    print(f"Event '{event_name}' not found in the user journey for User ID: {user_id}")
            except (KeyError, IndexError) as e:
                print(f"Error occurred for user ID: {user_id}. {str(e)}")
            except Exception as e:
                print(f"An error occurred for user ID: {user_id}. {str(e)}")

            for user_event_sets in consecutive_event_sets.values():
                for event_set in user_event_sets:
                    try:
                        common_paths_counter[tuple(event_set)] += 1
                    except Exception as e:
                        print(f"An error occurred while processing event set: {event_set}. {str(e)}")

        return {
            'consecutive_event_sets': consecutive_event_sets,
            'common_paths_counter': common_paths_counter
        }

if __name__== '__main__':

    features_to_use=['userId' or 'distinct_id', 'time' or 'mp_processing_time_ms', 'name' or 'currentScene' or '$name',
                    '$app_release', '$ios_app_release', '$android_app_version', '$ios_app_version']
    # data_directory = '/content/event_data
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