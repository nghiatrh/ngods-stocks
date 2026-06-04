from functools import lru_cache
from typing import Iterable

from fastembed import TextEmbedding

from .config import settings


@lru_cache(maxsize=1)
def _model() -> TextEmbedding:
    return TextEmbedding(model_name=settings.embedding_model)


def embed(texts: Iterable[str]) -> list[list[float]]:
    return [list(v) for v in _model().embed(list(texts))]


def embed_one(text: str) -> list[float]:
    return embed([text])[0]
