
from sklearn.datasets import make_blobs
import pandas as pd
def main():
    dataset, labels = make_blobs(n_samples=100000, n_features=7, cluster_std=5, random_state=42)
    print(dataset)
    dataset = pd.DataFrame(dataset)
    dataset.to_csv("7features_100Krows.txt", index=False, columns= None)


if __name__ == "__main__":
    main()