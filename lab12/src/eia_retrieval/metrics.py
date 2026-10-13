import math


def recall_at_k(retrieved: list[str], gold: list[str], k: int) -> float:
    return len(set(retrieved[:k]) & set(gold)) / len(gold)


def precision_at_k(retrieved: list[str], gold: list[str], k: int) -> float:
    return len(set(retrieved[:k]) & set(gold)) / k


def reciprocal_rank(retrieved: list[str], gold: list[str], k: int) -> float:
    for position, chunk_id in enumerate(retrieved[:k], start=1):
        if chunk_id in gold:
            return 1.0 / position
    return 0.0


def dcg_at_k(retrieved: list[str], gold: list[str], k: int) -> float:
    return sum(
        1.0 / math.log2(position + 1)
        for position, chunk_id in enumerate(retrieved[:k], start=1)
        if chunk_id in gold
    )


def ideal_dcg_at_k(gold_count: int, k: int) -> float:
    return sum(1.0 / math.log2(position + 1) for position in range(1, min(gold_count, k) + 1))


def ndcg_at_k(retrieved: list[str], gold: list[str], k: int) -> float:
    ideal = ideal_dcg_at_k(len(gold), k)
    return dcg_at_k(retrieved, gold, k) / ideal if ideal else 0.0