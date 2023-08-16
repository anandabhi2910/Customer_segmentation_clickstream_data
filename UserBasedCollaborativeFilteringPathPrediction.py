"""# User based Collaborative filtering"""

from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler
import pandas as pd
import csv
from DataAnalyzer import user_journeys,distinct_ids

class UserBasedCollaborativeFiltering:
    def __init__(self, user_journeys):
        self.user_journeys = user_journeys

    def preprocess_data(self):
        try:
            # Prepare the data for user-based collaborative filtering
            user_sequences = []
            user_ids = []
            for user_id, journey in self.user_journeys.items():
                events = [event['current_scene'] for event in journey]
                user_sequences.append(events)
                user_ids.append(user_id)

            # Convert user sequences to a numerical representation (one-hot encoding)
            from sklearn.preprocessing import MultiLabelBinarizer
            mlb = MultiLabelBinarizer()
            user_sequences_encoded = mlb.fit_transform(user_sequences)

            # Scale the data for collaborative filtering
            scaler = StandardScaler()
            scaled_user_sequences = scaler.fit_transform(user_sequences_encoded)

            # Calculate the cosine similarity matrix
            similarity_matrix = cosine_similarity(scaled_user_sequences)

            return user_ids, similarity_matrix

        except Exception as e:
            print(f"An error occurred during data preprocessing: {str(e)}")

    def predict_next_scene(self, target_user_id, user_ids, similarity_matrix):
        try:
            # Find the index of the target user in the user_ids list
            target_user_index = user_ids.index(target_user_id)

            # Sort users based on similarity score in descending order
            similar_users_indices = (-similarity_matrix[target_user_index]).argsort()[1:]

            # Get the IDs of the most similar users
            similar_user_ids = [user_ids[idx] for idx in similar_users_indices]

            # Find the next scene based on the most similar users' paths
            for user_id in similar_user_ids:
                if user_id != target_user_id:
                    return self.user_journeys[user_id][-1]['current_scene']

            # If no similar user has a different scene, return the last scene of the target user's journey
            return self.user_journeys[target_user_id][-1]['current_scene']

        except Exception as e:
            print(f"An error occurred while predicting the next scene: {str(e)}")

# Example usage:
if __name__=='__main__':
    # user_journeys = {user_journeys}  # user_journeys data
    collaborative_filtering = UserBasedCollaborativeFiltering(user_journeys)
    distinct_ids, similarity_matrix = collaborative_filtering.preprocess_data()

    # Predict the next scene for a target user "64"
    target_user_id = '64'  # Replace with the desired target user ID
    predicted_next_scene = collaborative_filtering.predict_next_scene(int(target_user_id), distinct_ids, similarity_matrix)

    print(f"Predicted next scene for {target_user_id}: {predicted_next_scene}")