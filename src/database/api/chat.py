from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel
from database.auth import get_current_user
from database.mongo import chat_sessions_collection
from database.models import ChatSession, ChatMessage
from app.helpers import get_points_by_ids, B2_UPLOADER
from pathlib import Path
from urllib.parse import quote
from io import BytesIO
import os

FILE_SERVER_URL = os.getenv("FILE_SERVER_URL", "http://localhost:8000")

router = APIRouter(prefix="/chats", tags=["Chat Sessions"])

# ----------------------------
# CREATE (self or admin)
# ----------------------------
@router.post("/", status_code=201)
async def create_chat(session: ChatSession, current_user: dict = Depends(get_current_user)):
    session.user_id = str(current_user["id"])  # ensure stored as string
    await chat_sessions_collection.insert_one(session.to_mongo())
    return {
        "message": "Chat session created successfully",
        "id": session.id,
        "title": session.title,
    }

# ----------------------------
# GET ALL (admin)
# ----------------------------
@router.get("/", response_model=List[ChatSession])
async def get_all(current_user: dict = Depends(get_current_user)):
    if str(current_user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    cursor = chat_sessions_collection.find({}, {"_id": 0})
    chats = await cursor.to_list(length=1000)
    return chats

# ----------------------------
# GET MINE (self) - with pagination
# ----------------------------
class PaginatedChatsResponse(BaseModel):
    chats: List[ChatSession]
    total: int
    page: int
    limit: int
    has_more: bool

@router.get("/me", response_model=PaginatedChatsResponse)
async def get_my_chats(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    limit: int = Query(20, ge=1, le=100, description="Items per page (max 100)")
):
    user_id = str(current_user["id"])
    
    # Calculate skip
    skip = (page - 1) * limit
    
    # Get total count
    total = await chat_sessions_collection.count_documents({"user_id": user_id})
    
    # Get paginated results, sorted by created_at descending (newest first)
    # This ensures new chats appear at the top of the sidebar
    cursor = chat_sessions_collection.find(
        {"user_id": user_id}, 
        {"_id": 0}
    ).sort("created_at", -1).skip(skip).limit(limit)
    
    chats = await cursor.to_list(length=limit)
    
    return PaginatedChatsResponse(
        chats=chats,
        total=total,
        page=page,
        limit=limit,
        has_more=(skip + limit) < total
    )

# ----------------------------
# GET BY ID (admin or self) - with context files
# ----------------------------
async def get_context_files(context_ids: List[str]) -> List[dict]:
    """Get context files from context IDs by fetching from Qdrant and downloading from B2."""
    if not context_ids:
        return []
    
    try:
        from app.helpers import QDRANT_CLIENT, COLLECTION_NAME
        from qdrant_client import models
        
        # Search Qdrant by chunk_id in payload (not point ID)
        # We need to use scroll with filter since retrieve uses point IDs
        contexts = []
        for context_id in context_ids:
            try:
                # Use scroll with filter to find points by chunk_id
                scroll_result, _ = QDRANT_CLIENT.scroll(
                    collection_name=COLLECTION_NAME,
                    scroll_filter=models.Filter(
                        must=[
                            models.FieldCondition(
                                key="chunk_id",
                                match=models.MatchValue(value=context_id)
                            )
                        ]
                    ),
                    limit=1,
                    with_payload=True,
                    with_vectors=False
                )
                
                if scroll_result:
                    point = scroll_result[0]
                    payload = point.payload or {}
                    contexts.append({
                        "id": str(payload.get("chunk_id", point.id)),
                        "source_pdf": payload.get("source_pdf"),
                        "path": payload.get("path"),
                    })
            except Exception as e:
                print(f"Error fetching context {context_id}: {e}")
                continue
        
        downloaded_files = []
        seen_files = set()  # Track unique files to avoid duplicates
        
        # Use asyncio to run blocking B2 downloads in executor
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
        # Check if files already exist locally first
        DOWNLOADS_DIR = Path("downloads")
        
        async def download_single_file(context, source_pdf):
            """Download a single file from B2 in a thread pool, or use existing file."""
            try:
                # The source_pdf from B2 is the full path in B2, we need to extract just the filename
                # B2_UPLOADER.download_file expects the full B2 path and saves it as filename
                b2_filename = source_pdf.split("/")[-1] if "/" in source_pdf else source_pdf
                
                # Check if file already exists locally (B2_UPLOADER saves to downloads/filename)
                local_path = DOWNLOADS_DIR / b2_filename
                if local_path.exists():
                    print(f"✅ File already exists locally: {local_path}")
                    local_pdf_path = local_path
                else:
                    # Download from B2 in thread pool
                    print(f"⬇️ Downloading {source_pdf} from B2...")
                    loop = asyncio.get_event_loop()
                    local_pdf = await asyncio.wait_for(
                        loop.run_in_executor(
                            None,  # Use default executor
                            B2_UPLOADER.download_file, 
                            source_pdf
                        ),
                        timeout=30.0
                    )
                    local_pdf_path = Path(local_pdf)
                    print(f"✅ Downloaded to: {local_pdf_path}")
                
                # Create relative URL path
                relative_path = f"downloads/{local_pdf_path.name}"
                url = f"{FILE_SERVER_URL}/{quote(relative_path.replace(os.sep, '/'))}"
                
                file_info = {
                    "type": "pdf",
                    "path": str(local_pdf_path),
                    "url": url,
                    "name": context.get("path", b2_filename)
                }
                print(f"📄 File info: {file_info}")
                return file_info
            except asyncio.TimeoutError:
                print(f"⏱️ Timeout downloading PDF {source_pdf}")
                return {
                    "type": "pdf",
                    "path": source_pdf,
                    "url": None,
                    "name": context.get("path", source_pdf.split("/")[-1] if "/" in source_pdf else source_pdf)
                }
            except Exception as e:
                print(f"Error downloading PDF {source_pdf}: {e}")
                import traceback
                traceback.print_exc()
                # Return file info even if download fails
                return {
                    "type": "pdf",
                    "path": source_pdf,
                    "url": None,
                    "name": context.get("path", source_pdf.split("/")[-1] if "/" in source_pdf else source_pdf)
                }
        
        # Download files concurrently
        download_tasks = []
        for context in contexts:
            source_pdf = context.get("source_pdf")
            if source_pdf and source_pdf not in seen_files:
                seen_files.add(source_pdf)
                download_tasks.append(download_single_file(context, source_pdf))
        
        # Execute all downloads concurrently
        if download_tasks:
            results = await asyncio.gather(*download_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception):
                    print(f"Download task failed: {result}")
                    import traceback
                    traceback.print_exc()
                    continue
                if result:
                    downloaded_files.append(result)
        
        return downloaded_files
    except Exception as e:
        print(f"Error getting context files: {e}")
        import traceback
        traceback.print_exc()
        return []

@router.get("/{chat_id}")
async def get_chat(chat_id: str, current_user: dict = Depends(get_current_user)):
    chat = await chat_sessions_collection.find_one({"id": chat_id}, {"_id": 0})
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if str(current_user["id"]) != str(chat.get("user_id")) and str(current_user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Access denied")

    # Process messages to add downloaded_files from contexts_used
    if chat.get("messages") and isinstance(chat["messages"], list):
        for message in chat["messages"]:
            context_ids = message.get("contexts_used", [])
            if context_ids:
                # Get context files for this message
                print(f"🔍 Fetching files for message with {len(context_ids)} context IDs: {context_ids[:3]}...")
                downloaded_files = await get_context_files(context_ids)
                print(f"✅ Retrieved {len(downloaded_files)} files for message")
                message["downloaded_files"] = downloaded_files
            elif "downloaded_files" not in message:
                message["downloaded_files"] = []
    
    # Debug: Print first message's files
    if chat.get("messages") and len(chat["messages"]) > 0:
        first_msg = chat["messages"][0]
        print(f"📄 First message has {len(first_msg.get('downloaded_files', []))} files")
        if first_msg.get("downloaded_files"):
            print(f"   Files: {[f.get('name') for f in first_msg['downloaded_files']]}")

    return chat

# ----------------------------
# DOWNLOAD CHAT AS PDF
# ----------------------------
@router.get("/{chat_id}/download")
async def download_chat_pdf(chat_id: str, current_user: dict = Depends(get_current_user)):
    """Download entire chat as PDF."""
    chat = await chat_sessions_collection.find_one({"id": chat_id}, {"_id": 0})
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if str(current_user["id"]) != str(chat.get("user_id")) and str(current_user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Access denied")

    # Process messages to add downloaded_files from contexts_used
    if chat.get("messages") and isinstance(chat["messages"], list):
        for message in chat["messages"]:
            context_ids = message.get("contexts_used", [])
            if context_ids:
                downloaded_files = await get_context_files(context_ids)
                message["downloaded_files"] = downloaded_files
            elif "downloaded_files" not in message:
                message["downloaded_files"] = []

    # Find logo path - try multiple locations
    logo_path = None
    current_file = Path(__file__).resolve()
    
    # Try different possible locations
    possible_paths = [
        # Direct absolute path
        Path(r"D:\fastcite\fastcite_frontend\public\logo.png"),
        # Relative to current file (database/api/chat.py)
        current_file.parent.parent.parent.parent / "fastcite_frontend" / "public" / "logo.png",
        current_file.parent.parent.parent.parent / "fastcite_frontend" / "src" / "assets" / "logo.png",
        # Absolute paths from project root
        Path("fastcite_frontend/public/logo.png"),
        Path("fastcite_frontend/src/assets/logo.png"),
        Path("../fastcite_frontend/public/logo.png"),
        Path("../fastcite_frontend/src/assets/logo.png"),
        # From backend directory
        current_file.parent.parent.parent / "logo.png",
        # From project root
        current_file.parent.parent.parent.parent / "logo.png",
    ]
    
    for path in possible_paths:
        try:
            if path.exists() and path.is_file():
                logo_path = str(path.resolve())
                print(f"✅ Found logo at: {logo_path}")
                break
        except Exception:
            continue
    
    if not logo_path:
        print("⚠️ Logo not found, PDF will be generated without logo")
        print(f"   Searched in: {[str(p) for p in possible_paths[:4]]}")

    # Generate PDF
    from services.pdf_generator import ChatPDFGenerator
    generator = ChatPDFGenerator(logo_path=logo_path)
    
    # Prepare chat data for PDF
    # Convert datetime objects to ISO format strings if needed
    created_at = chat.get("created_at")
    if created_at and not isinstance(created_at, str):
        if hasattr(created_at, 'isoformat'):
            created_at = created_at.isoformat()
        else:
            created_at = str(created_at)
    
    updated_at = chat.get("updated_at")
    if updated_at and not isinstance(updated_at, str):
        if hasattr(updated_at, 'isoformat'):
            updated_at = updated_at.isoformat()
        else:
            updated_at = str(updated_at)
    
    chat_data = {
        "title": chat.get("title", "Chat Conversation"),
        "created_at": created_at or datetime.utcnow().isoformat(),
        "updated_at": updated_at or datetime.utcnow().isoformat(),
        "messages": chat.get("messages", []),
        "book_name": chat.get("book_name")  # Add if available
    }
    
    pdf_stream = BytesIO()
    generator.generate_chat_pdf(chat_data, pdf_stream, include_all_messages=True)
    pdf_stream.seek(0)
    
    # Generate filename
    safe_title = "".join(c for c in chat_data["title"] if c.isalnum() or c in (' ', '-', '_')).rstrip()
    filename = f"{safe_title or 'chat'}_{chat_id[:8]}.pdf"
    
    return StreamingResponse(
        pdf_stream,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# ----------------------------
# DOWNLOAD SINGLE Q&A AS PDF
# ----------------------------
@router.get("/{chat_id}/download/{message_index}")
async def download_message_pdf(
    chat_id: str, 
    message_index: int,
    current_user: dict = Depends(get_current_user)
):
    """Download a single Q&A pair as PDF."""
    chat = await chat_sessions_collection.find_one({"id": chat_id}, {"_id": 0})
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if str(current_user["id"]) != str(chat.get("user_id")) and str(current_user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Access denied")

    messages = chat.get("messages", [])
    if message_index < 0 or message_index >= len(messages):
        raise HTTPException(status_code=404, detail="Message not found")

    # Process the specific message to add downloaded_files
    message = messages[message_index]
    context_ids = message.get("contexts_used", [])
    if context_ids:
        downloaded_files = await get_context_files(context_ids)
        message["downloaded_files"] = downloaded_files
    else:
        message["downloaded_files"] = []

    # Find logo path - try multiple locations
    logo_path = None
    current_file = Path(__file__).resolve()
    
    # Try different possible locations
    possible_paths = [
        # Direct absolute path
        Path(r"D:\fastcite\fastcite_frontend\public\logo.png"),
        # Relative to current file (database/api/chat.py)
        current_file.parent.parent.parent.parent / "fastcite_frontend" / "public" / "logo.png",
        current_file.parent.parent.parent.parent / "fastcite_frontend" / "src" / "assets" / "logo.png",
        # Absolute paths from project root
        Path("fastcite_frontend/public/logo.png"),
        Path("fastcite_frontend/src/assets/logo.png"),
        Path("../fastcite_frontend/public/logo.png"),
        Path("../fastcite_frontend/src/assets/logo.png"),
        # From backend directory
        current_file.parent.parent.parent / "logo.png",
        # From project root
        current_file.parent.parent.parent.parent / "logo.png",
    ]
    
    for path in possible_paths:
        try:
            if path.exists() and path.is_file():
                logo_path = str(path.resolve())
                print(f"✅ Found logo at: {logo_path}")
                break
        except Exception:
            continue
    
    if not logo_path:
        print("⚠️ Logo not found, PDF will be generated without logo")
        print(f"   Searched in: {[str(p) for p in possible_paths[:4]]}")

    # Generate PDF
    from services.pdf_generator import ChatPDFGenerator
    generator = ChatPDFGenerator(logo_path=logo_path)
    
    # Prepare chat data for PDF (only include the selected message)
    # Convert datetime objects to ISO format strings if needed
    created_at = chat.get("created_at")
    if created_at and not isinstance(created_at, str):
        if hasattr(created_at, 'isoformat'):
            created_at = created_at.isoformat()
        else:
            created_at = str(created_at)
    
    updated_at = chat.get("updated_at")
    if updated_at and not isinstance(updated_at, str):
        if hasattr(updated_at, 'isoformat'):
            updated_at = updated_at.isoformat()
        else:
            updated_at = str(updated_at)
    
    chat_data = {
        "title": f"{chat.get('title', 'Chat Conversation')} - Q&A {message_index + 1}",
        "created_at": created_at or datetime.utcnow().isoformat(),
        "updated_at": updated_at or datetime.utcnow().isoformat(),
        "messages": [message],  # Only include the selected message
        "book_name": chat.get("book_name")
    }
    
    pdf_stream = BytesIO()
    generator.generate_chat_pdf(chat_data, pdf_stream, include_all_messages=True)
    pdf_stream.seek(0)
    
    # Generate filename
    safe_title = "".join(c for c in chat.get("title", "chat") if c.isalnum() or c in (' ', '-', '_')).rstrip()
    filename = f"{safe_title or 'chat'}_qa_{message_index + 1}_{chat_id[:8]}.pdf"
    
    return StreamingResponse(
        pdf_stream,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

# ----------------------------
# APPEND MESSAGE
# ----------------------------
@router.post("/{chat_id}/message")
async def add_message(chat_id: str, message: ChatMessage, current_user: dict = Depends(get_current_user)):
    chat = await chat_sessions_collection.find_one({"id": chat_id})
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if str(current_user["id"]) != str(chat.get("user_id")) and str(current_user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Access denied")

    await chat_sessions_collection.update_one(
        {"id": chat_id},
        {
            "$push": {"messages": message.dict()},
            "$set": {"updated_at": datetime.utcnow()}
        }
    )
    return {"message": "Message added successfully"}

# ----------------------------
# DELETE CHAT
# ----------------------------
@router.delete("/user/{user_id}/all")
async def delete_all_user_chats(user_id: str, current_user: dict = Depends(get_current_user)):
    """
    Delete all chat sessions for a specific user.
    Only the user themselves can delete their own chats (for account deletion).
    """
    # Verify user can only delete their own chats
    if str(current_user["id"]) != user_id:
        raise HTTPException(status_code=403, detail="You can only delete your own chats")
    
    try:
        # Delete all chats for this user
        result = await chat_sessions_collection.delete_many({"user_id": user_id})
        
        return {
            "message": f"Deleted {result.deleted_count} chat sessions for user",
            "deleted_count": result.deleted_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete user chats: {str(e)}")


@router.delete("/{chat_id}")
async def delete_chat(chat_id: str, current_user: dict = Depends(get_current_user)):
    chat = await chat_sessions_collection.find_one({"id": chat_id})
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")

    if str(current_user["id"]) != str(chat.get("user_id")) and str(current_user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Access denied")

    await chat_sessions_collection.delete_one({"id": chat_id})
    return {"message": "Chat deleted successfully"}
