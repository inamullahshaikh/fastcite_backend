from fastapi import APIRouter, HTTPException, Depends
from celery_app.tasks import search_similar_in_books_task, process_contexts_and_generate_task
from app.helpers import AIMODEL, B2_UPLOADER
import os
from pydantic import BaseModel
from database.mongo import books_collection, chat_sessions_collection
from database.auth import get_current_user
from datetime import datetime
from pathlib import Path
from app.embedder import embedder
from urllib.parse import quote
from app.helpers import clear_downloads_folder

router = APIRouter(prefix="/rag", tags=["PDF Upload & Delete"])

class QueryMultipleBooksRequest(BaseModel):
    """Model for a RAG query scoped to multiple books."""
    prompt: str
    book_id: str
    top_k: int = 3
    chat_session_id: str

async def book_exists(book_id: str) -> bool:
    """Checks if a book with the given ID exists in the MongoDB collection."""
    book = await books_collection.find_one({"id": book_id})
    return book is not None

FILE_SERVER_URL = os.getenv("FILE_SERVER_URL", "http://localhost:8000")

@router.post("/query")
async def rag_multiple_books_answer(
    request: QueryMultipleBooksRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Perform a RAG query scoped to a list of specific books.
    Returns answer, reasoning, and context information.
    Optionally saves Q&A to a chat session.
    """
    if not request.book_id:
        raise HTTPException(status_code=400, detail="book_id cannot be empty.")

    # Validate all book IDs exist
    if isinstance(request.book_id, list):
        missing_books = [bid for bid in request.book_id if not await book_exists(bid)]
    else:
        missing_books = [] if await book_exists(request.book_id) else [request.book_id]

    if missing_books:
        raise HTTPException(
            status_code=404,
            detail=f"Books not found: {', '.join(missing_books)}"
        )

    # NEW: Validate chat session if provided
    chat_session = None
    if request.chat_session_id:
        chat_session = await chat_sessions_collection.find_one(
            {"id": request.chat_session_id}
        )
        if not chat_session:
            raise HTTPException(status_code=404, detail="Chat session not found")
        
        # Verify user owns the chat session or is admin
        if current_user["role"] != "admin" and str(chat_session["user_id"]) != str(current_user["id"]):
            raise HTTPException(status_code=403, detail="Access denied to this chat session")

    top_k = 10
    # Embed the user prompt (convert to list for JSON serialization)
    query_vec = embedder.embed(request.prompt)[0].tolist()
    
    # ----------------------------
    # Step 1: Search for similar chunks in specified books
    # ----------------------------
    try:
        search_task = search_similar_in_books_task.delay(query_vec, request.book_id, top_k)
        contexts = search_task.get(timeout=60)  # list of dicts
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Search task timed out.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search task failed: {str(e)}")

    if not contexts:
        contexts = []

    # ----------------------------
    # Step 2: Generate answer using selected contexts via LLM
    # ----------------------------
    try:
        llm_task = process_contexts_and_generate_task.delay(contexts, request.prompt)
        task_result = llm_task.get(timeout=60)
    except TimeoutError:
        raise HTTPException(status_code=504, detail="LLM generation task timed out.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"LLM generation failed: {str(e)}")

    answer_text = task_result.get("answer", "")
    reasoning_text = task_result.get("reasoning", "")
    selected_contexts = task_result.get("selected_contexts", [])
    selected_ids = task_result.get("selected_ids", [])

    # ----------------------------
    # Step 3: Download PDFs for selected contexts
    # ----------------------------
    downloaded_files = []
    # clear_downloads_folder()
    for c in selected_contexts:
        source_pdf = c.get("source_pdf")
        if source_pdf:
            try:
                local_pdf = B2_UPLOADER.download_file(source_pdf)
                local_pdf_path = Path(local_pdf)

                # Create relative URL path
                relative_path = f"downloads/{local_pdf_path.name}"
                url = f"{FILE_SERVER_URL}/{quote(relative_path.replace(os.sep, '/'))}"
                downloaded_files.append({
                    "type": "pdf",
                    "path": str(local_pdf),
                    "url": url,
                    "name": c["path"]
                })
            except Exception as e:
                print(f"Error downloading PDF {source_pdf}: {e}")

    # Combine selected context text for debugging / display
    context_text = "\n\n".join([
        f"### {c.get('heading','')}\n{c.get('content','')}"
        for c in selected_contexts
    ])

    # Context metadata provided to LLM (top-k)
    all_contexts_info = [
        {
            "id": c.get('id'),
            "book_id": c.get('book_id'),
            "book_name": c.get('book_name'),
            "heading": c.get('heading'),
            "score": c.get('score')
        }
        for c in contexts[:top_k]
    ]

    # ----------------------------
    # NEW: Step 4: Save message to chat session if provided
    # ----------------------------
    if request.chat_session_id and chat_session:
        try:
            new_message = {
                "question": request.prompt,
                "answer": answer_text,
                "timestamp": datetime.utcnow(),
                "reasoning": reasoning_text,  # Optional: include reasoning
                "contexts_used": selected_ids  # Optional: track which contexts were used
            }
            
            await chat_sessions_collection.update_one(
                {"id": request.chat_session_id},
                {
                    "$push": {"messages": new_message},
                    "$set": {"updated_at": datetime.utcnow()}
                }
            )
            
            # Update chat title with first question if it's still "New Chat"
            if len(chat_session.get("messages", [])) == 0 and chat_session.get("title") == "New Chat":
                # Generate a title from the first question (first 50 chars)
                title = request.prompt[:50] + ("..." if len(request.prompt) > 50 else "")
                await chat_sessions_collection.update_one(
                    {"id": request.chat_session_id},
                    {"$set": {"title": title}}
                )
        except Exception as e:
            print(f"Error saving message to chat session: {e}")
            # Don't fail the request if chat save fails

    return {
        "answer": answer_text,
        "reasoning": reasoning_text,
        "context_used": context_text,
        "selected_context_ids": selected_ids,
        "contexts_count": len(selected_contexts),
        "contexts_provided_to_llm": all_contexts_info,
        "contexts_selected_by_llm": selected_ids,
        "downloaded_files": downloaded_files,
        "model": AIMODEL,
        "book_searched": request.book_id,
        "chat_session_id": request.chat_session_id  # Return the session ID
    }