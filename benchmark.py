import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.feature_extraction.text import HashingVectorizer, TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.metrics import jaccard_score
import faiss
from collections import defaultdict
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA

# ========================
# Настройки
# ========================
CLUSTER_LIFETIME = 3600  # секунда (1 час)
CLUSTER_GRACE = 300      # секунда (5 минут)
SIM_THRESHOLD = 0.7      # порог схожести для объединения в кластер
INPUT_FILE = "events_stream.json"
OUTPUT_CSV = "benchmark_results.csv"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")


# ========================
# Классы кластеризации
# ========================
class Cluster:
    def __init__(self, vector, event_time, text):
        self.vectors = [vector]
        self.texts = [text]
        self.centroid = vector
        self.last_update = event_time
        self.created_at = event_time
        self.closed = False

    def update(self, vector, event_time, text):
        self.vectors.append(vector)
        self.texts.append(text)
        self.centroid = np.mean(self.vectors, axis=0)
        self.last_update = event_time


class SimpleClusterizer:
    def __init__(self, method_name, lifetime=CLUSTER_LIFETIME, grace=CLUSTER_GRACE, threshold=SIM_THRESHOLD):
        self.clusters = []
        self.method_name = method_name
        self.lifetime = lifetime
        self.grace = grace
        self.threshold = threshold

    def process(self, vectors, times, texts):
        for i, vector in enumerate(vectors):
            event_time = times[i]
            text = texts[i]

            assigned = False
            for cluster in self.clusters:
                # проверяем время жизни кластера
                age = (event_time - cluster.last_update).total_seconds()
                if age > self.lifetime + self.grace:
                    continue

                sim = cosine_similarity(vector.reshape(1, -1), cluster.centroid.reshape(1, -1))[0, 0]
                if sim >= self.threshold:
                    cluster.update(vector, event_time, text)
                    assigned = True
                    break

            if not assigned:
                self.clusters.append(Cluster(vector, event_time, text))
        return self.clusters


class FaissClusterizer:
    def __init__(self, name="FAISS", lifetime=CLUSTER_LIFETIME, grace=CLUSTER_GRACE, threshold=SIM_THRESHOLD):
        self.clusters = []
        self.name = name
        self.lifetime = lifetime
        self.grace = grace
        self.threshold = threshold

    def process(self, vectors, times, texts):
        if len(vectors) == 0:
            return []

        for i, vector in enumerate(vectors):
            event_time = times[i]
            text = texts[i]

            assigned = False
            if self.clusters:
                centroids = np.array([c.centroid for c in self.clusters]).astype('float32')
                vector_f = vector.astype('float32')
                faiss.normalize_L2(centroids)
                faiss.normalize_L2(vector_f.reshape(1, -1))
                index = faiss.IndexFlatIP(centroids.shape[1])
                index.add(centroids)
                D, I = index.search(vector_f.reshape(1, -1), 1)
                best_idx = I[0][0]
                sim = D[0][0]
                cluster = self.clusters[best_idx]

                age = (event_time - cluster.last_update).total_seconds()
                if age <= self.lifetime + self.grace and sim >= self.threshold:
                    cluster.update(vector, event_time, text)
                    assigned = True

            if not assigned:
                self.clusters.append(Cluster(vector, event_time, text))
        return self.clusters


# ========================
# Векторизация
# ========================
def vectorize_hashing(texts, n_features=1024):
    hv = HashingVectorizer(n_features=n_features, alternate_sign=False)
    return hv.fit_transform(texts).toarray()


def vectorize_tfidf(texts):
    tfidf = TfidfVectorizer()
    return tfidf.fit_transform(texts).toarray()


def vectorize_jaccard(texts):
    # создаём простую бинарную мешок-слов матрицу
    hv = HashingVectorizer(n_features=1024, alternate_sign=False, binary=True)
    return hv.fit_transform(texts).toarray()


# ========================
# Сохранение кластеров
# ========================
def save_clusters(system, method_name, clusters):
    filename = f"clusters_{system}_{method_name}.txt"
    with open(filename, "w", encoding="utf-8") as f:
        for i, c in enumerate(clusters):
            f.write(f"\n=== CLUSTER {i} ===\n")
            f.write(f"Размер: {len(c.texts)}\n")
            f.write("Примеры:\n")
            for t in c.texts[:10]:
                f.write(f"- {t}\n")
    logging.info(f"Сохранено: {filename}")


# ========================
# Графическая визуализация
# ========================
def plot_clusters(vectors, clusters, system, method):
    labels = []
    all_vectors = []
    for idx, c in enumerate(clusters):
        for v in c.vectors:
            all_vectors.append(v)
            labels.append(idx)
    all_vectors = np.array(all_vectors)

    if all_vectors.shape[0] == 0:
        return

    pca = PCA(n_components=2)
    reduced = pca.fit_transform(all_vectors)

    plt.figure(figsize=(8, 6))
    for cluster_id in set(labels):
        points = reduced[np.array(labels) == cluster_id]
        plt.scatter(points[:, 0], points[:, 1], label=f"C{cluster_id}", alpha=0.6)

    plt.title(f"{system} - {method}")
    plt.legend()
    plt.tight_layout()
    plt.savefig(f"plot_{system}_{method}.png")
    plt.close()
    logging.info(f"График сохранен: plot_{system}_{method}.png")


# ========================
# Главная функция
# ========================
def main():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    logging.info(f"Всего событий: {len(data)}")

    # группируем по system
    systems = defaultdict(list)
    for event in data:
        systems[event["system"]].append(event)

    for system, events in systems.items():
        logging.info(f"[SYSTEM] {system}: {len(events)} событий")

    results = []

    for system, events in systems.items():
        logging.info(f"\n=== ОБРАБОТКА СИСТЕМЫ: {system} ===")

        # подготовка текстов и времени
        texts = []
        times = []
        for e in events:
            payload = e.get("payload", {})
            combined = " ".join(str(payload.get(k, "")) for k in ["title", "description", "category", "severity", "class"])
            texts.append(combined)
            times.append(datetime.fromisoformat(e["time"]))

        # векторы и методы
        methods = []

        logging.info("Vectorizing: Hashing")
        vectors = vectorize_hashing(texts)
        methods.append(("Hashing+Cosine", vectors, SimpleClusterizer("Hashing+Cosine")))

        logging.info("Vectorizing: TF-IDF")
        vectors = vectorize_tfidf(texts)
        methods.append(("TFIDF+Cosine", vectors, SimpleClusterizer("TFIDF+Cosine")))

        logging.info("Vectorizing: Jaccard")
        vectors = vectorize_jaccard(texts)
        methods.append(("Jaccard", vectors, SimpleClusterizer("Jaccard")))

        logging.info("Vectorizing: Hashing (for FAISS)")
        vectors = vectorize_hashing(texts)
        vectors = vectors.astype('float32')  # 🔥 обязательно для FAISS
        methods.append(("FAISS", vectors, FaissClusterizer("FAISS")))

        # обработка кластеризации
        for name, vectors, clusterizer in methods:
            logging.info(f"Кластеризация методом: {name}")
            start = datetime.now()
            clusters = clusterizer.process(vectors, times, texts)
            end = datetime.now()
            elapsed = (end - start).total_seconds()

            # сбор статистики
            num_clusters = len(clusters)
            created = sum(len(c.vectors) for c in clusters)
            added = created - len(texts)
            results.append([system, name, elapsed, num_clusters, created, abs(added), created-abs(added)])

            # сохраняем кластеры и график
            # save_clusters(system, name, clusters)
            # plot_clusters(vectors, clusters, system, name)

    # сохраняем CSV
    df = pd.DataFrame(results, columns=["system", "method", "time_sec", "clusters", "created", "added", "final"])
    df.to_csv(OUTPUT_CSV, index=False)
    logging.info(f"\nИтог CSV: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()