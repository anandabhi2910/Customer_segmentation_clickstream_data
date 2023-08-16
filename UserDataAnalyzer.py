from collections import defaultdict, Counter
from datetime import datetime, timedelta
from DataAnalyzer import DataAnalyzer
from UserJourneyFinder import UserJourneyFinder
from UserJourneyFinder import data_directory

class UserDataAnalyzer:
    def __init__(self, user_journeys):
        self.user_journeys = user_journeys

    def analyze_user_data(self, time_threshold=0):
        time_durations = {}
        distinct_sessions = {}
        average_events_per_session = {}
        most_common_time_of_day = {}
        most_common_day_of_week = {}

        # Extract user journeys for different time periods
        # last_3_months = datetime.now() - timedelta(days=90)
        # three_to_six_months_ago = datetime.now() - timedelta(days=180)
        # six_to_twelve_months_ago = datetime.now() - timedelta(days=365)

        # user_journeys_last_3_months = {}
        # user_journeys_three_to_six_months = {}
        # user_journeys_six_to_twelve_months = {}

        for user_id, journey in self.user_journeys.items():
            timestamps = [event.get('timestamp') for event in journey]
            if timestamps:
                # Calculate time duration
                first_timestamp = min(timestamps)
                last_timestamp = max(timestamps)
                try:
                    first_datetime = datetime.strptime(str(first_timestamp), '%Y-%m-%d %H:%M:%S')
                    last_datetime = datetime.strptime(str(last_timestamp), '%Y-%m-%d %H:%M:%S')
                    time_duration = last_datetime - first_datetime
                    time_durations[user_id] = time_duration

                    # Find distinct sessions
                    sessions = []
                    current_session = [journey[0]]
                    for i in range(1, len(journey)):
                        current_event = journey[i]
                        previous_event = journey[i - 1]
                        current_timestamp = datetime.strptime(str(current_event['timestamp']), '%Y-%m-%d %H:%M:%S')
                        previous_timestamp = datetime.strptime(str(previous_event['timestamp']), '%Y-%m-%d %H:%M:%S')
                        time_diff = current_timestamp - previous_timestamp
                        if time_diff > time_threshold:
                            sessions.append(current_session)
                            current_session = [current_event]
                        else:
                            current_session.append(current_event)
                    sessions.append(current_session)
                    distinct_sessions[user_id] = sessions

                    # # Get the most recent timestamp for the user
                    # last_timestamp = max(timestamps)
                    # last_datetime = datetime.strptime(str(last_timestamp), '%Y-%m-%d %H:%M:%S')
                    # # Categorize the user journey based on the last timestamp
                    # if last_datetime >= last_3_months:
                    #     user_journeys_last_3_months[user_id] = journey
                    # elif last_3_months > last_datetime >= three_to_six_months_ago:
                    #     user_journeys_three_to_six_months[user_id] = journey
                    # elif three_to_six_months_ago > last_datetime >= six_to_twelve_months_ago:
                    #     user_journeys_six_to_twelve_months[user_id] = journey

                    # Find most common time of day
                    time_of_day_counts = Counter()
                    for event in journey:
                        timestamp = event.get('timestamp')
                        if timestamp:
                            time = datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').time()
                            if time.hour < 6:
                                time_of_day_counts['Night'] += 1
                            elif time.hour < 12:
                                time_of_day_counts['Morning'] += 1
                            elif time.hour < 18:
                                time_of_day_counts['Afternoon'] += 1
                            else:
                                time_of_day_counts['Evening'] += 1

                    most_common_time_of_day[user_id] = max(time_of_day_counts, key=time_of_day_counts.get)

                    # Find most common day of week
                    day_of_week_counts = Counter()
                    for event in journey:
                        timestamp = event.get('timestamp')
                        if timestamp:
                            date = datetime.strptime(str(timestamp), '%Y-%m-%d %H:%M:%S').date()
                            day_of_week = date.strftime('%A')
                            day_of_week_counts[day_of_week] += 1

                    most_common_day_of_week[user_id] = max(day_of_week_counts, key=day_of_week_counts.get)

                except ValueError:
                    print(f"Error: Failed to convert timestamps to datetime objects for user ID: {user_id}")
                except Exception as e:
                    print(f"Error: An error occurred while analyzing user data for user ID: {user_id}. {str(e)}")

        # Calculate average events per session
        for user_id, sessions in distinct_sessions.items():
            try:
                session_counts = [len(session) for session in sessions]
                average_events = sum(session_counts) / len(session_counts)
                average_events_per_session[user_id] = average_events
            except ZeroDivisionError:
                print(f"Error: No sessions found for user ID: {user_id}")
            except Exception as e:
                print(f"Error: An error occurred while calculating average events per session for user ID: {user_id}. {str(e)}")

        return {
            'time_durations': time_durations,
            'distinct_sessions': distinct_sessions,
            'average_events_per_session': average_events_per_session,
            'most_common_time_of_day': most_common_time_of_day,
            'most_common_day_of_week': most_common_day_of_week
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


