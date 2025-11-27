import os
import shutil
from pathlib import Path
from dotenv import load_dotenv
from b2sdk.v2 import InMemoryAccountInfo, B2Api
from qdrant_client import QdrantClient, models
from sentence_transformers import SentenceTransformer
import fitz
from google import genai
from google.genai.types import Content, Part
from typing import List, Dict
import re
from collections import Counter
print("Initializing global clients for Celery worker...")

# -------------------------------------------------------------------
# Environment Setup
# -------------------------------------------------------------------

load_dotenv()

# --- Backblaze B2 ---
B2_KEY_ID = os.getenv("B2_KEY_ID")
B2_APP_KEY = os.getenv("B2_APP_KEY")
B2_BUCKET_NAME = os.getenv("B2_BUCKET_NAME")

# --- Qdrant ---
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")

if not all([B2_KEY_ID, B2_APP_KEY, B2_BUCKET_NAME]):
    raise EnvironmentError("❌ Missing B2_KEY_ID, B2_APP_KEY, or B2_BUCKET_NAME in .env file")

if not QDRANT_URL:
    raise EnvironmentError("❌ Missing QDRANT_URL in .env file")

# Create downloads directory
DOWNLOADS_DIR = Path("downloads")
DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------------
# Utility Functions
# -------------------------------------------------------------------

def clear_downloads_folder():
    """Completely empty the downloads folder before new download."""
    if DOWNLOADS_DIR.exists():
        for item in DOWNLOADS_DIR.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        print("🧹 Cleared downloads folder")


# -------------------------------------------------------------------
# Backblaze B2 Uploader
# -------------------------------------------------------------------

class BackblazeUploader:
    """Handles file uploads, downloads, and deletions from Backblaze B2."""

    def __init__(self):
        key_id = os.getenv("B2_KEY_ID")
        app_key = os.getenv("B2_APP_KEY")
        self.bucket_name = os.getenv("B2_BUCKET_NAME")

        if not all([key_id, app_key, self.bucket_name]):
            raise EnvironmentError("❌ Missing B2_KEY_ID, B2_APP_KEY, or B2_BUCKET_NAME")

        info = InMemoryAccountInfo()
        self.b2_api = B2Api(info)
        self.b2_api.authorize_account("production", key_id, app_key)
        self.bucket = self.b2_api.get_bucket_by_name(self.bucket_name)

        print(f"✅ Connected to Backblaze B2 bucket: {self.bucket_name}")

    def upload_one(self, local_path, remote_name, info):
        """Upload a single file to B2 with metadata."""
        try:
            self.bucket.upload_local_file(
                local_file=local_path,
                file_name=remote_name,
                file_infos={
                    "book_id": info.get("book_id", "unknown"),
                    "book_name": info.get("book_name", "unknown"),
                    "author_name": info.get("author_name", "unknown"),
                },
            )
            print(f"✅ Uploaded: {remote_name}")
            return remote_name
        except Exception as e:
            print(f"⚠️ Upload failed for {local_path}: {e}")
            return None

    def list_files(self):
        """Generator to list all files in the bucket."""
        return self.bucket.ls()

    def delete_file(self, file_id, file_name):
        """Delete a specific file version."""
        self.b2_api.delete_file_version(file_id, file_name)

    def download_file(self, file_name: str):
        """Download a file from B2 if not already cached."""
        local_path = DOWNLOADS_DIR / file_name
        local_path.parent.mkdir(parents=True, exist_ok=True)

        print(f"⬇️ Downloading {file_name} from B2...")
        self.bucket.download_file_by_name(file_name).save_to(local_path)
        print(f"✅ Saved to {local_path}")

        return str(local_path)


# -------------------------------------------------------------------
# PDF Utility Functions
# -------------------------------------------------------------------

def build_toc_tree(toc):
    """Builds a nested TOC tree structure from PyMuPDF's table of contents."""
    root = {"title": "root", "page": 0, "children": []}
    stack = [root]
    for level, title, page in toc:
        node = {"title": title, "page": page, "children": []}
        while len(stack) > level:
            stack.pop()
        stack[-1]["children"].append(node)
        stack.append(node)
    return root


def collect_leaf_nodes(node, path=None):
    """Collects all leaf nodes from the TOC tree."""
    if path is None:
        path = []
    current_path = path + [node["title"]] if node["title"] != "root" else []

    if not node["children"]:
        return [{"title": node["title"], "page": node["page"], "path": current_path}]

    leaves = []
    for child in node["children"]:
        leaves.extend(collect_leaf_nodes(child, current_path))
    return leaves


def extract_metadata(doc):
    """Extract metadata (title, author) from a PDF document."""
    meta = doc.metadata or {}
    # Return None for missing title instead of "Unknown Title" to allow validation
    title = meta.get("title") or None
    
    # Ensure title is a string if it exists
    if title is not None:
        if not isinstance(title, str):
            # Handle case where title might be an object or other type
            if isinstance(title, dict):
                title = str(title.get("title", title.get("name", "")))
            else:
                title = str(title)
        # Strip whitespace and check if empty
        if title and title.strip():
            title = title.strip()
        else:
            title = None
    
    author = meta.get("author") or None
    # Ensure author is a string if it exists
    if author is not None:
        if not isinstance(author, str):
            if isinstance(author, dict):
                author = str(author.get("author", author.get("name", "")))
            else:
                author = str(author)
        if author and author.strip():
            author = author.strip()
        else:
            author = None
    
    return title, author


def extract_text_for_node(doc, start_page, end_page):
    """Extract text from a range of pages."""
    text = ""
    for pno in range(start_page - 1, end_page - 1):
        text += doc.load_page(pno).get_text("text") + "\n"
    return text.strip()


def save_mini_pdf(doc, start_page, end_page, output_dir, book_id):
    """Save a portion of a PDF (subset of pages) as a mini PDF."""
    os.makedirs(output_dir, exist_ok=True)
    mini_doc = fitz.open()
    for pno in range(start_page - 1, end_page - 1):
        mini_doc.insert_pdf(doc, from_page=pno, to_page=pno)

    pdf_filename = f"{book_id}_{start_page}_{end_page - 1}.pdf"
    output_path = os.path.join(output_dir, pdf_filename)

    mini_doc.save(output_path)
    mini_doc.close()
    return output_path, pdf_filename


# -------------------------------------------------------------------
# Global Client Instances
# -------------------------------------------------------------------

# Initialize Backblaze B2
B2_UPLOADER = BackblazeUploader()

# Initialize Qdrant client
try:
    QDRANT_CLIENT = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
    print(f"✅ Connected to Qdrant at: {QDRANT_URL}")
except Exception as e:
    print(f"❌ Failed to initialize Qdrant client: {e}")
    QDRANT_CLIENT = None
COLLECTION_NAME = "pdf_chunks"
AIMODEL = "gemini-2.5-flash-lite"
client_genai = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))


def filter_contexts_by_relevance(contexts: List[Dict], min_score: float = 0.3) -> List[Dict]:
    """
    Filter contexts that are below relevance threshold.
    
    Args:
        contexts: List of context dictionaries with 'score' field
        min_score: Minimum similarity score (0.0 to 1.0). Default 0.3.
    
    Returns:
        Filtered list of contexts that meet the relevance threshold
    """
    if not contexts:
        return []
    
    # Filter by score threshold
    relevant_contexts = [
        ctx for ctx in contexts 
        if ctx.get('score', 0) is not None and ctx.get('score', 0) >= min_score
    ]
    
    # If no contexts meet threshold, return empty list
    return relevant_contexts


def _format_context(hit):
    """Standardize context schema for all search functions based on current payload."""
    payload = hit.payload or {}
    return {
        "id": str(payload.get("chunk_id", hit.id)),  # fallback to hit.id
        "score": getattr(hit, "score", None),
        "book_id": payload.get("book_id"),
        "book_name": payload.get("book_name"),
        "book_author": payload.get("author_name"),  # updated field name
        "heading": payload.get("heading", ""),
        "content": payload.get("content", ""),
        "source_pdf": payload.get("source_pdf"),
        "source_images": payload.get("source_images", []),
        "start_page": payload.get("start_page"),
        "end_page": payload.get("end_page"),
        "path": payload.get("path"),
    }


def search_similar(query_vector, top_k=3):
    """Search Qdrant for semantically similar chunks globally."""
    if not QDRANT_CLIENT.collection_exists(COLLECTION_NAME):
        raise ValueError(f"Collection '{COLLECTION_NAME}' not found in Qdrant")

    info = QDRANT_CLIENT.get_collection(COLLECTION_NAME)
    if info.points_count == 0:
        raise ValueError(f"Collection '{COLLECTION_NAME}' is empty")

    search_result = QDRANT_CLIENT.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        limit=top_k,
        with_payload=True,
        with_vectors=False,
    )
    return [_format_context(hit) for hit in search_result.points]


def _extract_keywords(query: str) -> List[str]:
    """Extract meaningful keywords from a query string."""
    # Remove punctuation and convert to lowercase
    query_clean = re.sub(r'[^\w\s]', ' ', query.lower())
    # Split into words and filter out common stop words
    stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'should', 'could', 'may', 'might', 'must', 'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she', 'it', 'we', 'they', 'what', 'which', 'who', 'when', 'where', 'why', 'how'}
    words = [w for w in query_clean.split() if len(w) > 2 and w not in stop_words]
    return words


def _calculate_keyword_score(content: str, keywords: List[str]) -> float:
    """Calculate a keyword matching score for content based on keyword frequency."""
    if not keywords or not content:
        return 0.0
    
    content_lower = content.lower()
    keyword_counts = Counter()
    
    # Count occurrences of each keyword in the content
    for keyword in keywords:
        # Count word boundaries to avoid partial matches
        pattern = r'\b' + re.escape(keyword) + r'\b'
        matches = len(re.findall(pattern, content_lower))
        keyword_counts[keyword] = matches
    
    # Calculate score: sum of keyword frequencies, normalized
    total_matches = sum(keyword_counts.values())
    if total_matches == 0:
        return 0.0
    
    # Normalize by content length and number of keywords
    # Higher score for more keyword matches relative to content length
    content_length = len(content.split())
    if content_length == 0:
        return 0.0
    
    # Score: (total matches / content length) * (unique keywords matched / total keywords)
    unique_matched = len([k for k in keywords if keyword_counts[k] > 0])
    frequency_score = total_matches / max(content_length, 1)
    coverage_score = unique_matched / len(keywords) if keywords else 0
    
    # Combined score (weighted average)
    score = (frequency_score * 0.6) + (coverage_score * 0.4)
    return min(score, 1.0)  # Cap at 1.0


def search_keywords_in_book(query_text: str, book_id: str, top_k: int = 3) -> List[Dict]:
    """Search for chunks in a book using keyword matching (text search)."""
    if not QDRANT_CLIENT.collection_exists(COLLECTION_NAME):
        raise ValueError(f"Collection '{COLLECTION_NAME}' not found in Qdrant")

    info = QDRANT_CLIENT.get_collection(COLLECTION_NAME)
    if info.points_count == 0:
        raise ValueError(f"Collection '{COLLECTION_NAME}' is empty")

    # Extract keywords from query
    keywords = _extract_keywords(query_text)
    if not keywords:
        return []

    # Filter by book_id
    filter_cond = models.Filter(
        must=[
            models.FieldCondition(
                key="book_id",
                match=models.MatchAny(any=[book_id])
            )
        ]
    )

    # Scroll through all points matching the filter
    all_chunks = []
    next_page = None
    
    while True:
        batch, next_page = QDRANT_CLIENT.scroll(
            collection_name=COLLECTION_NAME,
            limit=100,
            with_payload=True,
            with_vectors=False,
            scroll_filter=filter_cond,
            offset=next_page
        )
        
        for point in batch:
            payload = point.payload or {}
            content = payload.get("content", "")
            if content:
                # Calculate keyword score
                keyword_score = _calculate_keyword_score(content, keywords)
                if keyword_score > 0:
                    context = _format_context(point)
                    context["score"] = keyword_score
                    all_chunks.append(context)
        
        if not next_page:
            break

    # Sort by keyword score and return top-k
    all_chunks.sort(key=lambda x: x.get("score", 0) or 0, reverse=True)
    return all_chunks[:top_k]


def search_similar_in_book(query_vector, book_id: str, top_k: int = 3):
    """Search top-k similar chunks in a single book using vector search."""
    if not QDRANT_CLIENT.collection_exists(COLLECTION_NAME):
        raise ValueError(f"Collection '{COLLECTION_NAME}' not found in Qdrant")

    info = QDRANT_CLIENT.get_collection(COLLECTION_NAME)
    if info.points_count == 0:
        raise ValueError(f"Collection '{COLLECTION_NAME}' is empty")

    # Match single book_id
    filter_cond = models.Filter(
        must=[
            models.FieldCondition(
                key="book_id",
                match=models.MatchAny(any=[book_id])  # ensure a list
            )
        ]
    )

    search_result = QDRANT_CLIENT.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vector,
        limit=top_k,
        with_payload=True,
        with_vectors=False,
        query_filter=filter_cond,
        search_params=models.SearchParams(hnsw_ef=128, exact=False)
    )

    return [_format_context(hit) for hit in search_result.points]


def hybrid_search_in_book(query_vector: List[float], query_text: str, book_id: str, top_k: int = 3, 
                          vector_weight: float = 0.6, keyword_weight: float = 0.4) -> List[Dict]:
    """
    Perform hybrid search combining vector search (60%) and keyword search (40%).
    
    Args:
        query_vector: Embedding vector for semantic search
        query_text: Original query text for keyword search
        book_id: Book ID to search within
        top_k: Number of top results to return
        vector_weight: Weight for vector search results (default 0.6)
        keyword_weight: Weight for keyword search results (default 0.4)
    
    Returns:
        List of context dictionaries with combined scores
    """
    # Perform both searches
    vector_results = search_similar_in_book(query_vector, book_id, top_k * 2)  # Get more results for merging
    keyword_results = search_keywords_in_book(query_text, book_id, top_k * 2)
    
    # Create a dictionary to store combined results by chunk ID
    combined_results: Dict[str, Dict] = {}
    
    # Normalize vector scores (Qdrant returns cosine similarity, typically 0-1)
    if vector_results:
        vector_scores = [r.get("score", 0) or 0 for r in vector_results]
        max_vector_score = max(vector_scores) if vector_scores else 1.0
        min_vector_score = min(vector_scores) if vector_scores else 0.0
        vector_range = max_vector_score - min_vector_score if max_vector_score > min_vector_score else 1.0
        
        for result in vector_results:
            chunk_id = result.get("id")
            if chunk_id:
                # Normalize vector score to 0-1 range
                raw_score = result.get("score", 0) or 0
                normalized_score = (raw_score - min_vector_score) / vector_range if vector_range > 0 else 0.5
                combined_results[chunk_id] = {
                    **result,
                    "vector_score": normalized_score,
                    "keyword_score": 0.0
                }
    
    # Normalize keyword scores (already 0-1 range from our function)
    if keyword_results:
        keyword_scores = [r.get("score", 0) or 0 for r in keyword_results]
        max_keyword_score = max(keyword_scores) if keyword_scores else 1.0
        min_keyword_score = min(keyword_scores) if keyword_scores else 0.0
        keyword_range = max_keyword_score - min_keyword_score if max_keyword_score > min_keyword_score else 1.0
        
        for result in keyword_results:
            chunk_id = result.get("id")
            if chunk_id:
                # Normalize keyword score to 0-1 range
                raw_score = result.get("score", 0) or 0
                normalized_score = (raw_score - min_keyword_score) / keyword_range if keyword_range > 0 else 0.5
                
                if chunk_id in combined_results:
                    # Update existing result with keyword score
                    combined_results[chunk_id]["keyword_score"] = normalized_score
                else:
                    # Add new result from keyword search
                    combined_results[chunk_id] = {
                        **result,
                        "vector_score": 0.0,
                        "keyword_score": normalized_score
                    }
    
    # Calculate combined scores
    final_results = []
    for chunk_id, result in combined_results.items():
        vector_score = result.get("vector_score", 0.0)
        keyword_score = result.get("keyword_score", 0.0)
        
        # Combine scores with weights
        combined_score = (vector_score * vector_weight) + (keyword_score * keyword_weight)
        
        result["score"] = combined_score
        # Remove intermediate score fields
        result.pop("vector_score", None)
        result.pop("keyword_score", None)
        final_results.append(result)
    
    # Sort by combined score and return top-k
    final_results.sort(key=lambda x: x.get("score", 0) or 0, reverse=True)
    return final_results[:top_k]


def get_points_by_ids(ids: list):
    """Fetch multiple points (chunks) from Qdrant by their chunk_ids."""
    if not ids:
        return []

    if not QDRANT_CLIENT.collection_exists(COLLECTION_NAME):
        raise ValueError(f"Collection '{COLLECTION_NAME}' not found in Qdrant")

    info = QDRANT_CLIENT.get_collection(COLLECTION_NAME)
    if info.points_count == 0:
        raise ValueError(f"Collection '{COLLECTION_NAME}' is empty")

    try:
        points = QDRANT_CLIENT.retrieve(
            collection_name=COLLECTION_NAME,
            ids=ids,
            with_payload=True,
            with_vectors=False
        )

        results = []
        for p in points:
            payload = p.payload or {}
            results.append({
                "id": str(payload.get("chunk_id", p.id)),
                "book_id": payload.get("book_id"),
                "book_name": payload.get("book_name"),
                "book_author": payload.get("author_name"),
                "heading": payload.get("heading"),
                "content": payload.get("content"),
                "source_pdf": payload.get("source_pdf"),
                "source_images": payload.get("source_images", []),
                "start_page": payload.get("start_page"),
                "end_page": payload.get("end_page"),
                "path": payload.get("path"),
                "score": getattr(p, "score", None),
            })
        return results

    except Exception as e:
        print(f"Error fetching points by IDs: {e}")
        return []


def get_all(limit: int = 100):
    """Fetch all stored points from Qdrant collection."""
    if not QDRANT_CLIENT.collection_exists(COLLECTION_NAME):
        raise ValueError(f"Collection '{COLLECTION_NAME}' not found in Qdrant")

    info = QDRANT_CLIENT.get_collection(COLLECTION_NAME)
    if info.points_count == 0:
        raise ValueError(f"Collection '{COLLECTION_NAME}' is empty")

    try:
        points = []
        next_page = None

        while True:
            batch, next_page = QDRANT_CLIENT.scroll(
                collection_name=COLLECTION_NAME,
                limit=limit,
                with_payload=True,
                with_vectors=False,
                offset=next_page
            )
            points.extend([_format_context(p) for p in batch])
            if not next_page:
                break

        return {"count": len(points), "points": points}
    except Exception as e:
        return {"error": str(e)}
