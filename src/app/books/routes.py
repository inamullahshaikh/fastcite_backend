from fastapi import APIRouter, UploadFile, File, HTTPException, Depends, status, Form
from celery_app.tasks import process_pdf_task, delete_book_task
from database.auth import get_current_user, get_admin_user, users_collection
from services.email_service import email_service
import os
import uuid
import shutil
from celery_app.celery_app import celery_app
from celery.result import AsyncResult
from database.mongo import books_collection
router = APIRouter(prefix="/pdf", tags=["PDF Upload & Delete"])

UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# -------------------- Upload Route -------------------- #
@router.post("/upload")
async def upload_pdf(
    file: UploadFile = File(...),
    book_name: str = Form(None),
    current_user: dict = Depends(get_current_user)
):
    """
    Upload a PDF, associate it with the authenticated user, 
    and trigger background processing via Celery.
    
    Args:
        file: PDF file to upload
        book_name: Optional book name. Required if PDF metadata doesn't contain a title.
    """
    try:
        # Save file temporarily
        file_id = str(uuid.uuid4())
        file_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)

        user_id = str(current_user["id"])
        qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")
        # Enqueue Celery background task with user_id and optional book_name
        task = process_pdf_task.delay(pdf_path=file_path, user_id=user_id)

        # Send book upload email notification
        try:
            user = await users_collection.find_one({"id": user_id})
            if user:
                book_display_name = book_name or file.filename or "Untitled"
                email_service.send_book_uploaded_email(
                    user_email=user.get("email"),
                    user_name=user.get("name", user.get("username", "User")),
                    book_name=book_display_name,
                    book_id="processing"  # Book ID not available yet
                )
        except Exception as e:
            print(f"⚠️ Failed to send book upload email: {e}")

        return {
            "message": "✅ File uploaded successfully. Processing started.",
            "task_id": task.id,
            "filename": file.filename,
            "uploaded_by": user_id,
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"File upload failed: {e}")


# -------------------- Delete All User Books Route (must be before /{book_id}) -------------------- #
@router.delete("/user/{user_id}/all")
async def delete_all_user_books(user_id: str, current_user: dict = Depends(get_current_user)):
    """
    Delete all books for a specific user.
    Only the user themselves can delete their own books (for account deletion).
    """
    # Verify user can only delete their own books
    if str(current_user["id"]) != user_id:
        raise HTTPException(status_code=403, detail="You can only delete your own books")
    
    try:
        # Find all books where this user is in uploaded_by
        query = {
            "$or": [
                {f"uploaded_by.{user_id}": {"$exists": True}},  # New dict format
                {"uploaded_by": user_id}  # Old list format
            ]
        }
        
        # Get all books for this user
        cursor = books_collection.find(query)
        user_books = await cursor.to_list(length=10000)  # Large limit to get all
        
        deleted_count = 0
        task_ids = []
        
        # Delete each book
        for book in user_books:
            book_id = book.get("id")
            if not book_id:
                continue
            
            uploaded_by = book.get("uploaded_by", {})
            
            # Handle migration: if uploaded_by is a list, convert to dict
            if isinstance(uploaded_by, list):
                uploaded_by = {uid: book.get("title", "Untitled") for uid in uploaded_by}
            
            # Check if user is the only uploader
            if isinstance(uploaded_by, dict):
                if len(uploaded_by) == 1 and user_id in uploaded_by:
                    # User is the only uploader - delete completely
                    task = delete_book_task.delay(book_id=book_id, user_id=user_id)
                    task_ids.append(task.id)
                    deleted_count += 1
                elif user_id in uploaded_by:
                    # Multiple users - just remove this user
                    uploaded_by.pop(user_id, None)
                    await books_collection.update_one(
                        {"id": book_id},
                        {"$set": {"uploaded_by": uploaded_by}}
                    )
                    deleted_count += 1
            else:
                # Old list format - if only one user, delete completely
                if len(uploaded_by) == 1 and user_id in uploaded_by:
                    task = delete_book_task.delay(book_id=book_id, user_id=user_id)
                    task_ids.append(task.id)
                    deleted_count += 1
                elif user_id in uploaded_by:
                    # Remove user from list
                    uploaded_by.remove(user_id)
                    await books_collection.update_one(
                        {"id": book_id},
                        {"$set": {"uploaded_by": uploaded_by}}
                    )
                    deleted_count += 1
        
        return {
            "message": f"Deleted {deleted_count} books for user",
            "deleted_count": deleted_count,
            "task_ids": task_ids
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete user books: {str(e)}")


# -------------------- Delete Route -------------------- #
@router.delete("/{book_id}")
async def delete_pdf(book_id: str, current_user: dict = Depends(get_current_user)):
    """
    Delete a book uploaded by the current user or by admin.
    - Regular users: If the book is shared with other users → removes only the current user.
                     If the user is the only uploader → deletes from Mongo, Qdrant, and B2.
    - Admin: Always deletes the book completely and sends email to all users who uploaded it.
    """
    try:
        user_id = str(current_user["id"])
        is_admin = str(current_user.get("role")) == "admin"
        qdrant_url = os.getenv("QDRANT_URL", "http://localhost:6333")

        # Get book info before deletion for email
        book = await books_collection.find_one({"id": book_id})
        if not book:
            raise HTTPException(status_code=404, detail="Book not found")

        uploaded_by = book.get("uploaded_by", {})
        
        # Handle migration: if uploaded_by is a list, convert to dict
        if isinstance(uploaded_by, list):
            uploaded_by = {uid: book.get("title", "Untitled") for uid in uploaded_by}
        
        # If admin is deleting, delete completely and notify all users
        if is_admin:
            # Get all user IDs who uploaded this book
            user_ids = list(uploaded_by.keys()) if isinstance(uploaded_by, dict) else []
            
            # Get book title for email
            book_title = book.get("title", "Unknown Book")
            
            # Enqueue Celery deletion task (admin deletion always deletes completely)
            task = delete_book_task.delay(book_id=book_id, user_id=None)  # None means admin deletion
            
            # Send book deletion email notification to ALL users who uploaded this book
            emails_sent = 0
            for uid in user_ids:
                try:
                    user = await users_collection.find_one({"id": uid})
                    if user and user.get("email"):
                        # Get user's custom book name if available
                        user_book_name = uploaded_by.get(uid, book_title) if isinstance(uploaded_by, dict) else book_title
                        
                        email_service.send_book_deleted_by_admin_email(
                            user_email=user.get("email"),
                            user_name=user.get("name", user.get("username", "User")),
                            book_name=user_book_name
                        )
                        emails_sent += 1
                except Exception as e:
                    print(f"⚠️ Failed to send book deletion email to user {uid}: {e}")
            
            return {
                "message": "🧹 Admin deletion task started. All users notified.",
                "task_id": task.id,
                "book_id": book_id,
                "requested_by": user_id,
                "is_admin": True,
                "users_notified": emails_sent,
                "total_users": len(user_ids)
            }
        
        # Regular user deletion (existing behavior)
        else:
            # Check if user has access to this book
            if isinstance(uploaded_by, dict):
                if user_id not in uploaded_by:
                    raise HTTPException(status_code=403, detail="You don't have access to this book")
                book_name = uploaded_by.get(user_id, book.get("title", "Unknown Book"))
            else:
                # Old list format - user must be in the list
                if user_id not in uploaded_by:
                    raise HTTPException(status_code=403, detail="You don't have access to this book")
                book_name = book.get("title", "Unknown Book")

            # Enqueue Celery deletion task
            task = delete_book_task.delay(book_id=book_id, user_id=user_id)

            # Send book deletion email notification to the current user only
            try:
                user = await users_collection.find_one({"id": user_id})
                if user:
                    email_service.send_book_deleted_email(
                        user_email=user.get("email"),
                        user_name=user.get("name", user.get("username", "User")),
                        book_name=book_name
                    )
            except Exception as e:
                print(f"⚠️ Failed to send book deletion email: {e}")

            return {
                "message": "🧹 Deletion task started.",
                "task_id": task.id,
                "book_id": book_id,
                "requested_by": user_id,
                "is_admin": False
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Book deletion failed: {e}")


@router.get("/task/{task_id}")
async def get_task_status(task_id: str):
    """
    Check the current status or result of a Celery task.
    """
    task_result = AsyncResult(task_id, app=celery_app)
    
    response = {
        "task_id": task_id,
        "status": task_result.status,       # e.g. PENDING, STARTED, SUCCESS, FAILURE
    }
    
    if task_result.successful():
        response["result"] = task_result.result
    elif task_result.failed():
        # Return error information when task fails
        error_info = str(task_result.result) if task_result.result else None
        response["result"] = {
            "error": error_info,
            "traceback": task_result.traceback if hasattr(task_result, 'traceback') else None
        }
    else:
        response["result"] = None
    
    return response