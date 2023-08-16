import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import AgglomerativeClustering
import scipy.cluster.hierarchy as sch
import sys


# Factory Design Pattern
class CustomerSegmentationFactory:
    def create_segmentation(self, segmentation_type, data_file,features_to_include):
        if segmentation_type == "Behavioral":
            return BehavioralSegmentation(data_file)
        elif segmentation_type == "Demographic":
            return DemographicSegmentation(data_file,features_to_include)
        elif segmentation_type == "Psychographic":
            return PsychographicSegmentation(data_file,features_to_include)
        else:
            raise ValueError("Invalid segmentation type provided")


# Adaptor Design Pattern:

#For the Behavioral Segmentation
class BehavioralSegmentation:
    def __init__(self, user_journeys):
        self.user_journeys = user_journeys

    def perform_clustering(self):
        try:
            # Prepare the data for clustering
            user_sequences = []
            for user_id, journey in self.user_journeys.items():
                events = [event['current_scene'] for event in journey]
                user_sequences.append(events)

            # Converting the user sequences to a numerical representation using one-hot encoding
            df = pd.DataFrame({'categories': user_sequences})
            one_hot_encoded_sequences = pd.get_dummies(df.categories.apply(pd.Series).stack()).sum(level=0)

            # Scaling the data for clustering
            scaler = StandardScaler()
            scaled_sequences = scaler.fit_transform(one_hot_encoded_sequences)

            # Plotting the dendrograms to find the required number of clusters
            sys.setrecursionlimit(100000)
            linkage_matrix = sch.linkage(scaled_sequences, method='ward')
            dendrogram = sch.dendrogram(linkage_matrix)
            plt.xlabel('Users')
            plt.ylabel('Distance')
            plt.title('Dendrogram')
            plt.show()

            num_clusters = 3  # From the dendrogram, we can see that 3 number of clusters can be used

            # Clustering using Hierarchical Clustering, using the Agglomerative Custering
            hc = AgglomerativeClustering(n_clusters=num_clusters, affinity='euclidean', linkage='ward')
            cluster_labels = hc.fit_predict(scaled_sequences)

            # Create a dictionary to store user IDs and their corresponding cluster labels
            user_clusters = {}
            for i, user_id in enumerate(self.user_journeys.keys()):
                user_clusters[user_id] = cluster_labels[i]

            # Print the user ID and their corresponding cluster label
            print("Customer Segments:")
            for user_id, cluster_label in user_clusters.items():
                print(f"User ID: {user_id}, Cluster Label: {cluster_label}")

        except Exception as e:
            print(f"An error occurred: {str(e)}")


#Now for the Demographic Segmentation
class DemographicSegmentation:
    def __init__(self, data_file,features_to_include):
        self.data_file = data_file

    def perform_clustering(self, features_to_include):
        try:
            # Data Loading and Exploration
            chunk_size = 10000  # Define the chunk size for loading data
            event_data_chunks = pd.read_csv(self.data_file, usecols=features_to_include, chunksize=chunk_size)
            event_data = pd.concat(event_data_chunks)

            print(event_data.head())

            # Feature Scaling
            scaler = StandardScaler()
            scaled_demographic_data = scaler.fit_transform(event_data)

            # Hierarchical Clustering
            sys.setrecursionlimit(100000)
            dendrogram = sch.dendrogram(sch.linkage(scaled_demographic_data, method='ward'))
            plt.title('Dendrogram')
            plt.xlabel('Customers')
            plt.ylabel('Euclidean Distances')
            plt.show()

            # Determining the Number of Clusters from the dendrogram plot
            num_clusters = 4

            # Agglomerative Clustering
            hc = AgglomerativeClustering(n_clusters=num_clusters, affinity='euclidean', linkage='ward')
            cluster_labels = hc.fit_predict(scaled_demographic_data)

            # Analyzing the Clusters
            demographic_data = pd.DataFrame(event_data, columns=features_to_include)
            demographic_data['Cluster'] = cluster_labels
            # Cluster statistics
            cluster_statistics = demographic_data.groupby('Cluster').describe()
            print(cluster_statistics)

            # To Visualize the Clusters (If necessary)
            # sns.scatterplot(data=demographic_data, x='ageGroup', y='profession', hue='Cluster', palette='Set1')
            # plt.title('Demographic-based Customer Segmentation')
            # plt.xlabel('Age Group')
            # plt.ylabel('Profession')
            # plt.show()

        except Exception as e:
            print(f"An error occurred: {str(e)}")



#Now for Psychographic Segmentation
class PsychographicSegmentation:
    def __init__(self, data_file,features_to_include):
        self.data_file = data_file

    def perform_clustering(self, features_to_include):
        try:
            # Data Loading and Exploration
            chunk_size = 10000  # Define the chunk size for loading data
            event_data_chunks = pd.read_csv(self.data_file, usecols=features_to_include, chunksize=chunk_size)
            event_data = pd.concat(event_data_chunks)

            print(event_data.head())

            # Feature Scaling
            scaler = StandardScaler()
            scaled_psychographic_data = scaler.fit_transform(event_data)

            # Hierarchical Clustering
            dendrogram = sch.dendrogram(sch.linkage(scaled_psychographic_data, method='ward'))
            plt.title('Dendrogram')
            plt.xlabel('Customers')
            plt.ylabel('Euclidean Distances')
            plt.show()

            # Determining the Number of Clusters
            num_clusters = 3  # Define the desired number of clusters

            # Agglomerative Clustering
            hc = AgglomerativeClustering(n_clusters=num_clusters, affinity='euclidean', linkage='ward')
            cluster_labels = hc.fit_predict(scaled_psychographic_data)

            # Analyzing the Clusters
            psychographic_data = pd.DataFrame(event_data, columns=features_to_include)
            psychographic_data['Cluster'] = cluster_labels
            # Cluster statistics
            cluster_statistics = psychographic_data.groupby('Cluster').describe()
            print(cluster_statistics)

            # To Visualize the Clusters (If necessary)
            # sns.scatterplot(data=psychographic_data, x='profession', y='pronoun', hue='Cluster', palette='Set1')
            # plt.title('Psychographic-based Customer Segmentation')
            # plt.xlabel('Profession')
            # plt.ylabel('Pronoun')
            # plt.show()

        except Exception as e:
            print(f"An error occurred: {str(e)}")