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
from typing import List
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
    return meta.get("title", "Unknown Title"), meta.get("author", "Unknown Author")


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
AIMODEL = "gemini-2.5-pro"
client_genai = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))


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


def search_similar_in_book(query_vector, book_id: str, top_k: int = 3):
    """Search top-k similar chunks in a single book."""
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
