import os
import uuid
import fitz
import concurrent.futures
from typing import List, Dict
from qdrant_client import models
from celery_app.celery_app import celery_app
from database.mongo import books_collections as books_collection, users_collections as users_collection
from datetime import datetime
from qdrant_client.http.exceptions import UnexpectedResponse
import re
import json
import hashlib
from app.helpers import *
from app.embedder import embedder
from app.book_chunker import BookChunker
from services.email_service import email_service
from app.rate_limiter import check_rate_limit, record_api_call

# ------------------ Sub-tasks for PDF Processing (LOW PRIORITY - Background) ------------------ #

@celery_app.task(name="initialize_qdrant_collection_task", queue='uploads')
def initialize_qdrant_collection_task(collection_name: str = "pdf_chunks"):
    """Initialize Qdrant collection if it doesn't exist and ensure required indexes exist."""
    if not QDRANT_CLIENT:
        raise EnvironmentError("Qdrant client not initialized.")
    
    existing = [c.name for c in QDRANT_CLIENT.get_collections().collections]
    collection_created = False
    
    if collection_name not in existing:
        QDRANT_CLIENT.create_collection(
            collection_name=collection_name,
            vectors_config=models.VectorParams(size=768, distance=models.Distance.COSINE),  # all-mpnet-base-v2 produces 768-dim embeddings
        )
        print("✅ Qdrant collection created.")
        collection_created = True
    else:
        print("ℹ️ Using existing Qdrant collection.")
    
    # Ensure required indexes exist with explicit schema types
    # Qdrant requires explicit field_schema when collection is empty or can't auto-detect
    required_indexes = [
        # Critical string fields (KEYWORD)
        {"field_name": "book_id", "field_schema": models.PayloadSchemaType.KEYWORD},  # Critical for deletion operations
        {"field_name": "chunk_id", "field_schema": models.PayloadSchemaType.KEYWORD},
        {"field_name": "book_name", "field_schema": models.PayloadSchemaType.KEYWORD},
        {"field_name": "author_name", "field_schema": models.PayloadSchemaType.KEYWORD},
        {"field_name": "heading", "field_schema": models.PayloadSchemaType.KEYWORD},
        {"field_name": "path", "field_schema": models.PayloadSchemaType.KEYWORD},
        {"field_name": "source_pdf", "field_schema": models.PayloadSchemaType.KEYWORD},
        {"field_name": "content", "field_schema": models.PayloadSchemaType.KEYWORD},
        {"field_name": "related_paths", "field_schema": models.PayloadSchemaType.KEYWORD},  # Array of strings
        # Integer fields (INTEGER)
        {"field_name": "start_page", "field_schema": models.PayloadSchemaType.INTEGER},
        {"field_name": "end_page", "field_schema": models.PayloadSchemaType.INTEGER},
        {"field_name": "level", "field_schema": models.PayloadSchemaType.INTEGER},
    ]
    
    for idx_info in required_indexes:
        field_name = idx_info["field_name"]
        field_schema = idx_info["field_schema"]
        try:
            QDRANT_CLIENT.create_payload_index(
                collection_name=collection_name,
                field_name=field_name,
                field_schema=field_schema
            )
            print(f"✅ Created/verified index for '{field_name}'")
        except Exception as e:
            error_msg = str(e).lower()
            if "already exists" in error_msg or "duplicate" in error_msg:
                print(f"ℹ️ Index for '{field_name}' already exists")
            else:
                print(f"⚠️ Warning: Could not create index for '{field_name}': {e}")
    
    return {
        "status": "created" if collection_created else "exists",
        "collection_name": collection_name
    }


@celery_app.task(name="extract_pdf_metadata_task", queue='uploads')
def extract_pdf_metadata_task(pdf_path: str) -> Dict:
    """Extract metadata from PDF."""
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"{pdf_path} not found.")
    
    doc = fitz.open(pdf_path)
    try:
        toc = doc.get_toc()
        if not toc:
            raise ValueError("No Table of Contents found in PDF.")
        
        title, author_name = extract_metadata(doc)
        pages = doc.page_count
        
        return {
            "title": title,
            "author_name": author_name,
            "pages": pages,
            "toc": toc,
            "page_count": doc.page_count
        }
    finally:
        doc.close()


def create_toc_fingerprint(toc_tree: Dict) -> str:
    """
    Create a hash fingerprint from TOC tree structure for comparison.
    This helps identify books with the same TOC even when metadata is missing.
    
    Args:
        toc_tree: TOC tree dictionary
        
    Returns:
        SHA256 hash of the TOC structure
    """
    if not toc_tree:
        return ""
    
    # Create a normalized string representation of TOC structure
    def normalize_toc(node):
        """Recursively normalize TOC node to string."""
        if not node:
            return ""
        title = node.get("title", "")
        page = node.get("page", 0)
        children = node.get("children", [])
        children_str = "|".join(sorted([normalize_toc(child) for child in children]))
        return f"{title}:{page}[{children_str}]"
    
    toc_string = normalize_toc(toc_tree)
    return hashlib.sha256(toc_string.encode()).hexdigest()


@celery_app.task(name="check_or_create_book_task", queue='uploads')
def check_or_create_book_task(title: str, author_name: str, pages: int, user_id: str, user_book_name: str = None, toc_tree: Dict = None) -> Dict:
    """
    Check if book exists or create new book entry.
    
    Args:
        title: Title from PDF metadata (can be empty string)
        author_name: Author from PDF metadata
        pages: Number of pages
        user_id: ID of the user uploading
        user_book_name: User-provided book name (stored in uploaded_by dict)
        toc_tree: Table of contents tree
    """
    # Ensure title is a string
    if not isinstance(title, str):
        title = str(title) if title else ""
    title = title.strip()
    
    # Ensure author_name is a string
    if not isinstance(author_name, str):
        author_name = str(author_name) if author_name else ""
    author_name = author_name.strip()
    
    # Ensure user_book_name is a string
    if not user_book_name or not isinstance(user_book_name, str):
        user_book_name = str(user_book_name) if user_book_name else (title or "Untitled")
    user_book_name = user_book_name.strip() if user_book_name else (title or "Untitled")
    
    # Determine if we have metadata to match by
    has_metadata = title and title.strip()
    
    existing_book = None
    
    if has_metadata:
        # If we have title, match by title
        existing_book = books_collection.find_one({"title": title})
        print(f"🔍 Searching for book by title: {title}")
    elif toc_tree:
        # If no title but we have TOC, match by TOC fingerprint
        toc_fingerprint = create_toc_fingerprint(toc_tree)
        if toc_fingerprint:
            existing_book = books_collection.find_one({"toc_fingerprint": toc_fingerprint})
            print(f"🔍 Searching for book by TOC fingerprint (no metadata available)")
        else:
            print(f"⚠️ No TOC available and no metadata - treating as new book")
    else:
        # No title and no TOC - treat as new book
        print(f"⚠️ No metadata and no TOC - treating as new book")
    
    if existing_book:
        print(f"📚 Book already exists")
        if "id" not in existing_book:
            book_id = str(uuid.uuid4())
            books_collection.update_one(
                {"_id": existing_book["_id"]},
                {"$set": {"id": book_id}}
            )
            print(f"🔄 Migrated old book to UUID: {book_id}")
        else:
            book_id = existing_book["id"]
        
        # Update TOC and TOC fingerprint if provided and missing
        update_fields = {}
        if toc_tree and "toc" not in existing_book:
            update_fields["toc"] = toc_tree
            toc_fingerprint = create_toc_fingerprint(toc_tree)
            if toc_fingerprint:
                update_fields["toc_fingerprint"] = toc_fingerprint
            print(f"✅ Added TOC to existing book")
        elif toc_tree and "toc_fingerprint" not in existing_book:
            # Add fingerprint if missing
            toc_fingerprint = create_toc_fingerprint(toc_tree)
            if toc_fingerprint:
                update_fields["toc_fingerprint"] = toc_fingerprint
        
        if update_fields:
            books_collection.update_one(
                {"id": book_id},
                {"$set": update_fields}
            )
        
        # Update uploaded_by dict: add user with their book name
        uploaded_by = existing_book.get("uploaded_by", {})
        if not isinstance(uploaded_by, dict):
            # Migrate old list format to dict format
            uploaded_by = {uid: existing_book.get("title", "Untitled") for uid in (uploaded_by if isinstance(uploaded_by, list) else [])}
        
        # Add or update user's book name
        uploaded_by[user_id] = user_book_name or existing_book.get("title", "") or "Untitled"
        
        books_collection.update_one(
            {"id": book_id},
            {"$set": {"uploaded_by": uploaded_by}}
        )
        print(f"✅ Added user {user_id} to book with name: {uploaded_by[user_id]}")
        
        return {
            "book_id": book_id,
            "title": existing_book.get("title", title),  # Use existing title if available
            "author_name": existing_book.get("author_name", author_name),
            "status": "existing",
            "should_process": False
        }
    else:
        print(f"🆕 New book detected (PDF title: {title or 'No title'})")
        
        book_id = str(uuid.uuid4())
        
        # Create uploaded_by dict with user_id as key and user_book_name as value
        uploaded_by = {
            user_id: user_book_name or title or "Untitled"
        }
        
        new_book = {
            "id": book_id,
            "title": title,  # PDF metadata title (can be empty)
            "author_name": author_name,
            "pages": pages,
            "status": "processing",
            "uploaded_at": datetime.utcnow(),
            "uploaded_by": uploaded_by,  # Dict: {user_id: book_name}
        }
        
        # Add TOC tree and fingerprint if provided
        if toc_tree:
            new_book["toc"] = toc_tree
            toc_fingerprint = create_toc_fingerprint(toc_tree)
            if toc_fingerprint:
                new_book["toc_fingerprint"] = toc_fingerprint
            print(f"✅ Saving TOC tree and fingerprint for new book")
        
        books_collection.insert_one(new_book)
        print(f"✅ Saved book to MongoDB with uploaded_by: {uploaded_by}")
        
        return {
            "book_id": book_id,
            "title": title,
            "author_name": author_name,
            "status": "new",
            "should_process": True
        }


@celery_app.task(name="extract_pdf_chunks_task", queue='uploads')
def extract_pdf_chunks_task(pdf_path: str, toc: List, page_count: int, book_id: str, workers: int = 6) -> List[Dict]:
    """Extract and process PDF chunks using BookChunker."""
    # Initialize BookChunker with the provided book_id
    chunker = BookChunker(pdf_path, book_id=book_id)
    
    try:
        # Process chunks (this handles filtering, merging, and saving mini PDFs)
        chunks = chunker.process_chunks()
        
        # Transform chunks to match the expected pipeline format
        # Preserve all relevant fields from BookChunker schema
        pdf_chunks = []
        for chunk in chunks:
            # Extract filename from mini_pdf_path
            mini_pdf_path = chunk.get('mini_pdf_path', '')
            if mini_pdf_path:
                filename = os.path.basename(mini_pdf_path)
            else:
                # Fallback: generate filename if mini_pdf_path is missing
                filename = f"{book_id}_{chunk['start_page']}_{chunk['end_page']}.pdf"
            
            pdf_chunks.append({
                "chunkid": chunk.get("chunkid"),  # Preserve chunkid from BookChunker
                "title": chunk["title"],
                "path": chunk["path"],
                "level": chunk.get("level"),  # Preserve TOC level
                "start_page": chunk["start_page"],
                "end_page": chunk["end_page"],
                "text": chunk["text"],
                "related_paths": chunk.get("related_paths", []),  # Preserve related paths from merged chunks
                "local_path": mini_pdf_path,  # Full path to mini PDF
                "filename": filename,  # Just the filename for B2 upload
            })
        
        print(f"✅ Extracted {len(pdf_chunks)} chunks from PDF using BookChunker")
        return pdf_chunks
    finally:
        chunker.close()


@celery_app.task(name="upload_chunks_to_b2_task", queue='uploads')
def upload_chunks_to_b2_task(pdf_chunks: List[Dict], book_id: str, title: str, author_name: str, workers: int = 6) -> List[str]:
    """Upload PDF chunks to Backblaze B2."""
    if not B2_UPLOADER:
        raise EnvironmentError("B2 Uploader not initialized.")
    
    def upload_one(i):
        chunk = pdf_chunks[i]
        file_info = {"book_id": book_id, "book_name": title, "author_name": author_name}
        return i, B2_UPLOADER.upload_one(chunk["local_path"], chunk["filename"], file_info)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        all_urls = [url for _, url in executor.map(upload_one, range(len(pdf_chunks)))]
    
    print(f"✅ Uploaded {len([u for u in all_urls if u])} chunks to B2")
    return all_urls


@celery_app.task(name="generate_embeddings_task", queue='uploads')
def generate_embeddings_task(texts: List[str], batch_size: int = 50):
    """Generate embeddings for text chunks."""
    if not embedder:
        raise EnvironmentError("Embedder not initialized.")
    
    vectors = embedder.embed_batch(texts, batch_size=batch_size, show_progress_bar=True)
    print(f"✅ Generated embeddings for {len(texts)} chunks")
    return vectors


@celery_app.task(name="store_vectors_in_qdrant_task", queue='uploads')
def store_vectors_in_qdrant_task(
    chunks: List[Dict], 
    vectors, 
    urls: List[str], 
    book_id: str, 
    title: str, 
    author_name: str,
    batch_size: int = 50,
    collection_name: str = "pdf_chunks"
):
    """Store vectors and metadata in Qdrant using the new chunk schema."""
    if not QDRANT_CLIENT:
        raise EnvironmentError("Qdrant client not initialized.")
    
    points = []
    for chunk, vector, url in zip(chunks, vectors, urls):
        # Use chunkid from BookChunker if available, otherwise generate one
        chunk_id = chunk.get("chunkid") or str(uuid.uuid4())
        
        payload = {
            "chunk_id": chunk_id,  # Use chunkid from BookChunker
            "book_id": book_id,
            "book_name": title,
            "author_name": author_name,
            "start_page": chunk["start_page"],
            "end_page": chunk["end_page"],
            "heading": chunk["title"],
            "path": chunk["path"],
            "content": chunk["text"],
            "related_paths": chunk.get("related_paths", []),
            "source_pdf": url,
        }
        
        # Add optional fields if they exist
        if "level" in chunk:
            payload["level"] = chunk["level"]
        # Always save related_paths if it exists (even if empty list)
        # This ensures the field is consistently present in Qdrant for filtering
        if "related_paths" in chunk:
            payload["related_paths"] = chunk["related_paths"]
        
        points.append(models.PointStruct(id=uuid.uuid4().int >> 64, vector=vector.tolist(), payload=payload))

    for i in range(0, len(points), batch_size):
        QDRANT_CLIENT.upsert(collection_name=collection_name, points=points[i:i + batch_size])
    
    print(f"✅ Stored {len(points)} vectors in Qdrant")
    return len(points)


@celery_app.task(name="update_book_status_task", queue='uploads')
def update_book_status_task(book_id: str, status: str):
    """Update book processing status."""
    books_collection.update_one(
        {"id": book_id},
        {"$set": {"status": status}}
    )
    print(f"✅ Updated book {book_id} status to {status}")
    return {"book_id": book_id, "status": status}


# ------------------ Main PDF Processing Pipeline (LOW PRIORITY) ------------------ #

@celery_app.task(name="process_pdf_pipeline_task", queue='uploads')
def process_pdf_pipeline_task(
    pdf_path: str,
    metadata: Dict,
    book_info: Dict,
    batch_size: int = 50,
    workers: int = 6
):
    """
    Background pipeline for processing PDF chunks, uploading, and storing embeddings.
    This runs at LOW PRIORITY so chatbot queries are handled first.
    """
    
    # Step 1: Extract chunks
    pdf_chunks = extract_pdf_chunks_task(
        pdf_path,
        metadata["toc"],
        metadata["page_count"],
        book_info["book_id"],
        workers
    )
    
    # Step 2: Upload to B2
    all_urls = upload_chunks_to_b2_task(
        pdf_chunks,
        book_info["book_id"],
        book_info["title"],
        book_info["author_name"],
        workers
    )
    
    # Step 3: Filter valid chunks and generate embeddings
    valid_chunks = [c for c, u in zip(pdf_chunks, all_urls) if u]
    texts = [c["text"] for c in valid_chunks]
    vectors = generate_embeddings_task(texts, batch_size)
    
    # Step 4: Store in Qdrant
    chunk_count = store_vectors_in_qdrant_task(
        valid_chunks,
        vectors,
        [u for u in all_urls if u],
        book_info["book_id"],
        book_info["title"],
        book_info["author_name"],
        batch_size
    )
    
    # Step 5: Update status
    update_book_status_task(book_info["book_id"], "complete")
    
    # Step 6: Send email notification to all users who uploaded this book
    try:
        book = books_collection.find_one({"id": book_info["book_id"]})
        if book:
            uploaded_by = book.get("uploaded_by", {})
            # Handle both dict and list formats
            if isinstance(uploaded_by, list):
                user_ids = uploaded_by
            else:
                user_ids = list(uploaded_by.keys())
            
            # Get book display name
            book_display_name = book_info.get("title") or "Untitled"
            
            # Send email to each user
            for user_id in user_ids:
                try:
                    user = users_collection.find_one({"id": user_id})
                    if user and user.get("email"):
                        # Get user's custom book name if available
                        if isinstance(uploaded_by, dict):
                            user_book_name = uploaded_by.get(user_id, book_display_name)
                        else:
                            user_book_name = book_display_name
                        
                        email_service.send_book_uploaded_email(
                            user_email=user.get("email"),
                            user_name=user.get("name", user.get("username", "User")),
                            book_name=book_info["title"],
                            book_id=book_info["book_id"]
                        )
                except Exception as e:
                    print(f"⚠️ Failed to send completion email to user {user_id}: {e}")
    except Exception as e:
        print(f"⚠️ Failed to send book completion emails: {e}")
    
    print(f"✅ Completed background processing: {book_info['title']} by {book_info['author_name']}")
    return {
        "book_id": book_info["book_id"],
        "title": book_info["title"],
        "author_name": book_info["author_name"],
        "chunks": chunk_count
    }


@celery_app.task(name="process_pdf_to_qdrant_task", queue='uploads')
def process_pdf_task(pdf_path: str, user_id: str, batch_size: int = 50, workers: int = 6):
    """
    Main entry point for PDF upload. Quickly validates and creates book entry,
    then delegates heavy processing to background pipeline.
    
    Args:
        pdf_path: Path to the PDF file
        user_id: ID of the user uploading the book
        book_name: User-provided book name (stored in uploaded_by dict)
        batch_size: Batch size for processing
        workers: Number of workers for parallel processing
    """
    
    # Validate global clients
    if not all([embedder, QDRANT_CLIENT, B2_UPLOADER]):
        raise EnvironmentError("One or more global clients (Model, Qdrant, B2) are not initialized.")
    
    # Step 1: Initialize collection (quick operation)
    initialize_qdrant_collection_task()
    
    # Step 2: Extract metadata (quick operation)
    metadata = extract_pdf_metadata_task(pdf_path)
    
    # Step 2.1: Get title from PDF metadata (or empty string if not present)
    extracted_title = metadata.get("title")
    print(f"✅ Extracted title: {extracted_title}")
    # Ensure extracted_title is a string
    if extracted_title and not isinstance(extracted_title, str):
        if isinstance(extracted_title, dict):
            extracted_title = str(extracted_title.get("title", extracted_title.get("name", "")))
        else:
            extracted_title = str(extracted_title)
    
    # Use extracted title or empty string
    pdf_title = extracted_title.strip() if (extracted_title and extracted_title.strip()) else ""
    
    # Update metadata with PDF title
    metadata["title"] = pdf_title
    
    # Use user's book_name if provided, otherwise default to PDF title
    user_book_name = pdf_title
    
    # Step 2.5: Build TOC tree from raw TOC
    toc_tree = build_toc_tree(metadata["toc"])
    
    # Step 3: Check or create book (quick database operation)
    # Pass user's book_name to be stored in uploaded_by dict
    book_info = check_or_create_book_task(
        pdf_title,  # PDF metadata title (can be empty)
        metadata["author_name"],
        metadata["pages"],
        user_id,
        user_book_name,  # User-provided name (stored in uploaded_by)
        toc_tree
    )
    
    # If book already exists, return immediately
    if not book_info["should_process"]:
        print(f"✅ Book already exists: {book_info['title']}")
        return {
            "book_id": book_info["book_id"],
            "title": book_info["title"],
            "author_name": book_info["author_name"],
            "chunks": 0,
            "status": "existing"
        }
    
    # Delegate heavy processing to background pipeline (LOW PRIORITY)
    process_pdf_pipeline_task.delay(
        pdf_path,
        metadata,
        book_info,
        batch_size,
        workers
    )
    
    # Return immediately to user
    print(f"✅ Started background processing for book_id: {book_info['book_id']}")
    return {
        "book_id": book_info["book_id"],
        "title": book_info["title"],
        "author_name": book_info["author_name"],
        "status": "processing_started",
        "message": "Book is being processed in the background"
    }


# ------------------ DELETE TASKS (MEDIUM PRIORITY) ------------------ #

@celery_app.task(name="delete_qdrant_chunks_task", queue='maintenance')
def delete_qdrant_chunks_task(book_id: str):
    """Deletes all chunks in Qdrant using the global client."""
    
    if not QDRANT_CLIENT:
        print("❌ Qdrant client not initialized. Cannot delete chunks.")
        return {"status": "failed", "error": "Qdrant client not initialized"}
        
    collection_name = "pdf_chunks"

    try:
        # Ensure book_id index exists before deletion (required for filtering)
        try:
            QDRANT_CLIENT.create_payload_index(
                collection_name=collection_name,
                field_name="book_id"
            )
            print(f"✅ Created index for 'book_id' (required for deletion)")
        except Exception as idx_error:
            error_msg = str(idx_error).lower()
            if "already exists" in error_msg or "duplicate" in error_msg:
                print(f"ℹ️ Index for 'book_id' already exists")
            else:
                # If index creation fails for other reasons, log but continue
                print(f"⚠️ Warning: Could not create index for 'book_id': {idx_error}")
        
        print(f"🧹 Deleting Qdrant chunks for book_id={book_id}...")
        result = QDRANT_CLIENT.delete(
            collection_name=collection_name,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="book_id",
                            match=models.MatchValue(value=book_id)
                        )
                    ]
                )
            ),
        )
        print(f"✅ Deleted chunks from Qdrant for book_id={book_id}")
        return {"status": "success", "book_id": book_id}
    except Exception as e:
        print(f"❌ Failed to delete Qdrant chunks: {e}")
        return {"status": "failed", "error": str(e)}


@celery_app.task(name="delete_b2_pdfs_task", queue='maintenance')
def delete_b2_pdfs_task(book_id: str):
    """Deletes all mini-PDFs from Backblaze B2 using the global client."""
    
    if not B2_UPLOADER:
        print("❌ B2 Uploader not initialized. Cannot delete files.")
        return {"status": "failed", "error": "B2 Uploader not initialized"}

    try:
        print(f"🗑️ Deleting files from Backblaze B2 for book_id={book_id}...")

        deleted_count = 0
        for file_info, _ in B2_UPLOADER.list_files():
            file_metadata = file_info.file_info if hasattr(file_info, 'file_info') else {}
            if file_metadata.get("book_id") == book_id:
                try:
                    B2_UPLOADER.delete_file(file_info.id_, file_info.file_name)
                    print(f"✅ Deleted {file_info.file_name}")
                    deleted_count += 1
                except Exception as e:
                    print(f"⚠️ Failed to delete {file_info.file_name}: {e}")

        print(f"🧾 Completed cleanup for book_id={book_id}. Deleted {deleted_count} files.")
        return {"status": "success", "book_id": book_id, "deleted_count": deleted_count}

    except Exception as e:
        print(f"❌ Error deleting from B2: {e}")
        return {"status": "failed", "error": str(e)}


@celery_app.task(name="delete_book_task", queue='maintenance')
def delete_book_task(book_id: str, user_id: str):
    """Delete a book entry, with support for multi-user uploads."""
    
    book = books_collection.find_one({"id": book_id})
    if not book:
        return {"status": "not_found"}

    uploaded_by = book.get("uploaded_by", {})
    
    # Handle migration: if uploaded_by is a list, convert to dict
    if isinstance(uploaded_by, list):
        uploaded_by = {uid: book.get("title", "Untitled") for uid in uploaded_by}
        books_collection.update_one(
            {"id": book_id},
            {"$set": {"uploaded_by": uploaded_by}}
        )
    
    # If multiple users uploaded this book → only remove this user
    if len(uploaded_by) > 1:
        uploaded_by.pop(user_id, None)
        books_collection.update_one(
            {"id": book_id},
            {"$set": {"uploaded_by": uploaded_by}}
        )
        print(f"👤 Removed user {user_id} from book {book_id}")
        return {"status": "user_removed_only", "book_id": book_id}

    # If single uploader → delete everywhere
    books_collection.delete_one({"id": book_id})
    delete_qdrant_chunks_task.delay(book_id)
    delete_b2_pdfs_task.delay(book_id)
    
    print(f"🧹 Deleted book {book_id} completely")
    return {"status": "fully_deleted", "book_id": book_id}


# ------------------ CHATBOT TASKS (HIGH PRIORITY) ------------------ #

@celery_app.task(name="tasks.search_similar_in_books", queue='chatbot')
def search_similar_in_books_task(query_vec, query_text: str, book_id: str, top_k: int = 3):
    """
    HIGH PRIORITY: Search for similar content in books using hybrid search.
    Combines vector search (60%) and keyword search (40%).
    This task gets priority over PDF processing tasks.
    """
    all_results = []
    try:
        # Use hybrid search with 60% vector weight and 40% keyword weight
        all_results = hybrid_search_in_book(query_vec, query_text, book_id, top_k, vector_weight=0.6, keyword_weight=0.4)
    except UnexpectedResponse as e:
        print(f"UnexpectedResponse: {e}")
    except Exception as e:
        print(f"Error in search_similar_in_books: {e}")

    all_results.sort(key=lambda x: x.get('score', 0) or 0, reverse=True)
    return all_results


@celery_app.task(name="tasks.select_top_contexts", queue='chatbot', bind=True, max_retries=10)
def select_top_contexts_task(self, contexts: List[dict], user_query: str) -> List[str]:
    """
    HIGH PRIORITY: Use Gemini to pick top 3 most relevant contexts.
    This task gets priority over PDF processing tasks.
    Includes rate limiting to prevent exceeding 15 RPM limit.
    """
    # Check rate limit before making API call
    can_proceed, wait_seconds = check_rate_limit()
    
    if not can_proceed:
        # Rate limit reached, retry after wait_seconds
        print(f"⏸️ Rate limit reached in select_top_contexts_task. Retrying in {wait_seconds:.1f} seconds...")
        raise self.retry(countdown=int(wait_seconds) + 1, exc=Exception(f"Rate limit reached. Retry in {wait_seconds:.1f}s"))
    
    context_list = []
    for i, c in enumerate(contexts):
        context_id = c.get('id', f'unknown_{i}')
        heading = c.get('heading', 'No heading')
        content = c.get('content', '')[:500]
        context_list.append(f"ID: {context_id}\nHeading: {heading}\nContent: {content}...\n")
    contexts_text = "\n---\n".join(context_list)

    selection_prompt = f"""
    You are given {len(contexts)} context passages and a user query.
    Select the TOP 3 most relevant context passages.

    USER QUERY: {user_query}

    AVAILABLE CONTEXTS:
    {contexts_text}

    Respond with ONLY JSON:
    {{"selected_ids": ["id1", "id2", "id3"]}}
    """

    try:
        response = client_genai.models.generate_content(
            model=AIMODEL,
            contents=[
                Content(role="model", parts=[Part(text="You are an expert at evaluating context relevance.")]),
                Content(role="user", parts=[Part(text=selection_prompt)])
            ]
        )
        # Record successful API call
        record_api_call()
        
        cleaned = re.sub(r"^```json\s*|```$", "", response.text.strip(), flags=re.MULTILINE).strip()
        parsed = json.loads(cleaned)
        selected_ids = parsed.get("selected_ids", [])
        print("-----------------selected ids-------------------------")
        print(selected_ids)
        print("-----------------selected ids-------------------------")
        return selected_ids[:3] if len(selected_ids) >= 3 else selected_ids
    except Exception as e:
        print(f"Error selecting contexts: {e}")
        # If it's a rate limit error from Gemini API, retry
        if "429" in str(e) or "quota" in str(e).lower() or "rate limit" in str(e).lower():
            print(f"⏸️ Gemini API rate limit error. Retrying...")
            raise self.retry(countdown=60, exc=e)
        return [c.get('id') for c in contexts[:3]]


@celery_app.task(name="tasks.call_model", queue='chatbot', bind=True, max_retries=10)
def call_model_task(self, full_prompt: str, system_prompt: str) -> tuple[str, str]:
    """
    HIGH PRIORITY: Generate text using Gemini.
    This task gets priority over PDF processing tasks.
    Includes rate limiting to prevent exceeding 15 RPM limit.
    """
    # Check rate limit before making API call
    can_proceed, wait_seconds = check_rate_limit()
    
    if not can_proceed:
        # Rate limit reached, retry after wait_seconds
        print(f"⏸️ Rate limit reached in call_model_task. Retrying in {wait_seconds:.1f} seconds...")
        raise self.retry(countdown=int(wait_seconds) + 1, exc=Exception(f"Rate limit reached. Retry in {wait_seconds:.1f}s"))
    
    try:
        response = client_genai.models.generate_content(
            model=AIMODEL,
            contents=[
                Content(role="model", parts=[Part(text=system_prompt)]),
                Content(role="user", parts=[Part(text=full_prompt)])
            ]
        )
        # Record successful API call
        record_api_call()
        
        answer = response.text.strip()
        return answer, "No reasoning available (Gemini API does not return reasoning steps)"
    except Exception as e:
        error_str = str(e)
        print(f"Error calling Gemini model: {e}")
        
        # If it's a rate limit error from Gemini API, retry
        if "429" in error_str or "quota" in error_str.lower() or "rate limit" in error_str.lower():
            print(f"⏸️ Gemini API rate limit error. Retrying in 60 seconds...")
            raise self.retry(countdown=60, exc=e)
        
        return f"Error: {error_str}", "No reasoning available"


def _needs_current_information(query: str) -> bool:
    """
    Detect if a question requires current/real-time information (date, time, current events, current leaders).
    """
    query_lower = query.lower().strip()
    
    # Questions about current date/time
    date_time_patterns = [
        r'what.*today.*date',
        r'what.*current.*date',
        r'what.*time.*it',
        r'what.*time.*now',
        r'what.*day.*today',
        r'current.*date',
        r'today.*date',
        r'what.*date.*today',
        r'what.*is.*today',
        r'what.*day.*is.*it'
    ]
    if any(re.search(pattern, query_lower) for pattern in date_time_patterns):
        return True
    
    # Questions about current leaders/positions
    current_leader_patterns = [
        r'who.*president.*(usa|united states|america|us)',
        r'who.*prime minister.*(uk|britain|england|australia|canada|india)',
        r'who.*current.*president',
        r'who.*current.*leader',
        r'current.*president',
        r'current.*leader'
    ]
    if any(re.search(pattern, query_lower) for pattern in current_leader_patterns):
        return True
    
    # Questions about current events (recent news, current year, etc.)
    current_events_patterns = [
        r'what.*current.*year',
        r'what.*year.*is.*it',
        r'current.*year',
        r'recent.*news',
        r'latest.*news',
        r'current.*events'
    ]
    if any(re.search(pattern, query_lower) for pattern in current_events_patterns):
        return True
    
    return False


def _is_general_knowledge_question(query: str) -> bool:
    """
    Detect if a question is a general knowledge question that doesn't require document context.
    
    This should ONLY return True for questions that are clearly general knowledge:
    - Greetings: "hello", "hi", "hey"
    - Current date/time: "what is today's date?", "what time is it?"
    - Very common general facts that are unlikely to be in academic books
    
    We should be conservative - if in doubt, treat as book-specific question.
    """
    query_lower = query.lower().strip()
    
    # Very short queries (1-2 words) - check if they're greetings
    word_count = len(query_lower.split())
    if word_count <= 2:
        # Check if it's a greeting
        greetings = ['hello', 'hi', 'hey', 'greetings', 'good morning', 'good afternoon', 'good evening', 'goodbye', 'bye']
        if any(greeting in query_lower for greeting in greetings):
            return True
        # Single word queries that are clearly not book-related
        if word_count == 1 and query_lower in ['hello', 'hi', 'hey', 'thanks', 'thank you']:
            return True
    
    # Questions about current date/time - these are always general knowledge
    date_time_patterns = [
        r'what.*today.*date',
        r'what.*current.*date',
        r'what.*time.*it',
        r'what.*time.*now',
        r'what.*day.*today',
        r'current.*date',
        r'today.*date'
    ]
    if any(re.search(pattern, query_lower) for pattern in date_time_patterns):
        return True
    
    # Very specific general knowledge questions that are unlikely to be in books
    # These are questions about current events, common facts, etc.
    general_only_patterns = [
        r'what.*capital.*of.*(france|england|spain|germany|italy|japan|china|india|australia|canada|brazil|mexico)',
        r'who.*president.*(usa|united states|america)',
        r'what.*population.*of.*(world|earth)',
        r'how.*many.*people.*(world|earth|planet)'
    ]
    if any(re.search(pattern, query_lower) for pattern in general_only_patterns):
        return True
    
    # Default: treat as book-specific question (be conservative)
    return False


@celery_app.task(name="tasks.process_contexts_and_generate", queue='chatbot')
def process_contexts_and_generate_task(contexts: List[dict], user_query: str):
    """
    HIGH PRIORITY: Complete pipeline - select top contexts + generate answer.
    This task gets priority over PDF processing tasks.
    
    Now includes relevance filtering and conditional context usage.
    Improved handling of general knowledge questions.
    """
    from celery_app.tasks import select_top_contexts_task, call_model_task
    from app.helpers import filter_contexts_by_relevance

    # Check if this is a general knowledge question
    is_general_question = _is_general_knowledge_question(user_query)
    
    # Step 1: Filter contexts by relevance score
    # For general questions, use a higher threshold to ensure contexts are truly relevant
    # For book-specific questions, use the standard threshold
    min_score = 0.7 if is_general_question else 0.3
    filtered_contexts = filter_contexts_by_relevance(contexts, min_score=min_score)
    
    # Step 2: For general questions, skip context selection if scores are low
    # Even if contexts pass the threshold, check if they're actually relevant
    if is_general_question and filtered_contexts:
        # Check if the top context has a really high score (>0.8)
        # If not, it's likely not relevant to the general question
        top_score = filtered_contexts[0].get('score', 0) if filtered_contexts else 0
        if top_score < 0.8:
            # Contexts are not highly relevant, treat as general question
            filtered_contexts = []
    elif not is_general_question:
        # For book-specific questions, use contexts even if scores are moderate
        # This ensures we use book content for questions like "what is blockchain"
        pass
    
    # Step 3: Select top contexts from filtered results
    if filtered_contexts and not is_general_question:
        selection_contexts = filtered_contexts[:10]
        selected_ids = select_top_contexts_task(selection_contexts, user_query)
        selected_contexts = [c for c in selection_contexts if c.get('id') in selected_ids]
    else:
        # No relevant contexts found OR it's a general question
        selected_contexts = []
        selected_ids = []

    # Check if question needs current information (date, time, current events)
    needs_current_info = _needs_current_information(user_query)
    
    # Get current date/time if needed
    current_info = ""
    if needs_current_info:
        now = datetime.now()
        current_date = now.strftime("%B %d, %Y")  # e.g., "January 15, 2025"
        current_time = now.strftime("%I:%M %p")  # e.g., "02:30 PM"
        current_day = now.strftime("%A")  # e.g., "Monday"
        current_year = now.strftime("%Y")  # e.g., "2025"
        
        current_info = f"""
**Current Information:**
- Today's date: {current_date}
- Current day: {current_day}
- Current time: {current_time}
- Current year: {current_year}

Please use this current information to answer the question accurately. If the question asks about the current date, time, or year, use the information provided above."""

    # Step 4: Build prompt conditionally based on whether we have relevant contexts
    if selected_contexts and not is_general_question:
        # Use contexts when available and relevant (and not a general question)
        context_text = "\n\n".join([
            f"### {c.get('heading','')}\n{c.get('content','')}" 
            for c in selected_contexts
        ])
        full_prompt = f"""Use the following context from the documents to answer the question. If the context doesn't contain relevant information to answer the question, you may use your general knowledge instead.
{current_info}
Context:
{context_text}

**User Question:** {user_query}"""
    else:
        # No contexts OR it's a general question - use base knowledge
        if is_general_question:
            full_prompt = f"""Answer the following question using your general knowledge. This is a general knowledge question that does not require information from any documents.
{current_info}
**User Question:** {user_query}

Provide a helpful, accurate answer based on your training data and the current information provided above (if applicable). Do not mention that you cannot find the information in documents."""
        else:
            full_prompt = f"""Answer the following question using your general knowledge. Do not reference any documents or source materials.
{current_info}
**User Question:** {user_query}"""

    system_prompt = (
        "You are a knowledgeable AI assistant. Respond in clean Markdown with headings, bullet points, and summary. "
        "For general knowledge questions (greetings, current date/time, general facts), answer directly from your knowledge. "
        "When current date/time information is provided, use it to answer questions accurately. "
        "Do not say you cannot answer or that information is not available - provide the best answer you can."
    )

    answer, reasoning = call_model_task(full_prompt, system_prompt)
    return {
        "answer": answer,
        "reasoning": reasoning,
        "selected_ids": selected_ids,
        "selected_contexts": selected_contexts,
        "contexts_used": len(selected_contexts) > 0
    }