"""
Integration module connecting Ingestion (Kafka/MinIO) with ML Scoring
"""
from .kafka_consumer_scorer import RealTimeCreditScorer

__all__ = ["RealTimeCreditScorer"]
