import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.datasets import make_swiss_roll
from mpl_toolkits.mplot3d import Axes3D
from sklearn.cluster import KMeans


#delete the init parameter of KMeans to run an experiment using different centroids than ours
def main():
    data = pd.read_csv('3features_10Krows.txt', sep=",", header=None).astype("float")
    data.columns = ["feature1", "feature2", "feature3"]
    starting_centroids = [[1.7110094, -2.7438505, -0.24982078],
                          [-8.555084, -0.6983356, -6.100144],
                          [1.7632881, 5.7679462, -0.33050638],
                          [-6.9222307, -3.672655, 0.88910127],
                          [8.220542, 0.84488386, 1.4148422],
                          [9.160104, -9.031565, 5.0930467],
                          [-2.526917, 9.652638, 2.3013523],
                          [7.6660643, -6.6027536, -0.80693674]]

    kmeans = KMeans(n_clusters=8, n_init=1, max_iter=50, tol=0.001, init=starting_centroids)
    kmeans.set_params()
    kmeans.fit(data)
    print(kmeans.cluster_centers_)
    print(kmeans.inertia_)


def evaluateKmeans():
    data = pd.read_csv('3features_10Krows.txt', sep=",", header=None)
    data.columns = ["feature1", "feature2", "feature3"]
    centroids = [[1.7110094, -2.7438505, -0.24982078],
                 [-8.555084, -0.6983356, -6.100144],
                 [1.7632881, 5.7679462, -0.33050638],
                 [-6.9222307, -3.672655, 0.88910127],
                 [8.220542, 0.84488386, 1.4148422],
                 [9.160104, -9.031565, 5.0930467],
                 [-2.526917, 9.652638, 2.3013523],
                 [7.6660643, -6.6027536, -0.80693674]]
    print(calculate_inertia(centroids, data.to_numpy()))


def calculate_inertia(centroids, data_points):
    inertia = 0.0
    for i in range(len(data_points)):
        distances = np.sum((data_points[i] - centroids) ** 2, axis=1)
        inertia += np.min(distances)

    return inertia


def representClusters():
    cluster_points = pd.read_csv('5features_1Krows.txt', sep=",", header=None, index_col=False)
    cluster_points.columns = ["feature1", "feature2", "feature3", "feature4", "feature5"]
    kmeans = KMeans(n_clusters=4, n_init=1, max_iter=50, tol=0.001)
    kmeans.fit(cluster_points)
    cluster_points["Target"] = kmeans.labels_
    centroids = kmeans.cluster_centers_

    target_A = cluster_points[cluster_points['Target'] == 0]
    target_B = cluster_points[cluster_points['Target'] == 1]
    target_C = cluster_points[cluster_points['Target'] == 2]
    target_D = cluster_points[cluster_points['Target'] == 3]

    # Create 3D scatter plot with different colors based on target values
    fig = plt.figure()
    ax = fig.add_subplot(111, projection='3d')

    ax.scatter(target_A['feature1'], target_A['feature2'], target_A['feature3'], color='blue', label='0')
    ax.scatter(target_B['feature1'], target_B['feature2'], target_B['feature3'], color='red', label='1')
    ax.scatter(target_C['feature1'], target_C['feature2'], target_C['feature3'], color='black', label='2')
    ax.scatter(target_D['feature1'], target_D['feature2'], target_D['feature3'], color='purple', label='3')

    ax.scatter(centroids[0][0], centroids[0][1], centroids[0][2], color='black', marker="X")
    ax.scatter(centroids[1][0], centroids[1][1], centroids[1][2], color='black', marker="X")
    ax.scatter(centroids[2][0], centroids[2][1], centroids[2][2], color='black', marker="X")
    ax.scatter(centroids[3][0], centroids[3][1], centroids[3][2], color='black', marker="X")

    ax.set_xlabel('feature1')
    ax.set_ylabel('feature2')
    ax.set_zlabel('feature3')
    ax.set_title('3D Scatter Plot with Different Colors Based on Target')

    ax.legend()
    plt.grid(True)
    plt.show()


if __name__ == '__main__':
    representClusters()
