from sentence_transformers import SentenceTransformer

class Embedder:
    def __init__(self, model_name: str = "all-mpnet-base-v2", device: str = "cpu"):
        self.model = SentenceTransformer(model_name, device=device)

    def embed(self, texts):
        if isinstance(texts, str):
            texts = [texts]
        return self.model.encode(texts, convert_to_numpy=True, normalize_embeddings=True)

    def embed_batch(self, texts, batch_size: int = 64, show_progress_bar: bool = True):
        if isinstance(texts, str):
            texts = [texts]
        return self.model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=show_progress_bar,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )

# ✅ Create a global instance you can reuse anywhere
embedder = Embedder()
