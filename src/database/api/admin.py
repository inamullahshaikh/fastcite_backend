"""
Admin API - Complete Database Control
Provides full CRUD operations on all database collections for admin users.
"""
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime, timedelta
from database.auth import get_admin_user, get_current_user, hash_password
from database.mongo import users_collection
from database.mongo import (
    db, 
    users_collection, 
    chat_sessions_collection, 
    books_collection
)
from database.models import User, Book, ChatSession

router = APIRouter(prefix="/admin", tags=["Admin"])

# ==============================
# PYDANTIC MODELS
# ==============================
class AdminStatsResponse(BaseModel):
    total_users: int
    total_books: int
    total_chats: int
    active_users_30d: int
    books_by_status: Dict[str, int]
    users_by_role: Dict[str, int]

class PaginatedUsersResponse(BaseModel):
    users: List[Dict]
    total: int
    page: int
    limit: int
    has_more: bool

class PaginatedBooksResponse(BaseModel):
    books: List[Dict]
    total: int
    page: int
    limit: int
    has_more: bool

class PaginatedChatsResponse(BaseModel):
    chats: List[Dict]
    total: int
    page: int
    limit: int
    has_more: bool

class BulkOperationRequest(BaseModel):
    operation: str  # "delete", "update", "export"
    collection: str  # "users", "books", "chats"
    filters: Optional[Dict[str, Any]] = None
    updates: Optional[Dict[str, Any]] = None

class CollectionQueryRequest(BaseModel):
    collection: str
    filters: Optional[Dict[str, Any]] = None
    sort: Optional[Dict[str, int]] = None
    limit: Optional[int] = 100
    skip: Optional[int] = 0

# ==============================
# ADMIN STATISTICS
# ==============================
@router.get("/stats", response_model=AdminStatsResponse)
async def get_admin_stats(admin_user: dict = Depends(get_admin_user)):
    """Get comprehensive statistics about the database."""
    try:
        # Count total users
        total_users = await users_collection.count_documents({})
        
        # Count total books
        total_books = await books_collection.count_documents({})
        
        # Count total chats
        total_chats = await chat_sessions_collection.count_documents({})
        
        # Count active users in last 30 days
        thirty_days_ago = datetime.utcnow() - timedelta(days=30)
        active_users_30d = await users_collection.count_documents({
            "last_login": {"$gte": thirty_days_ago}
        })
        
        # Books by status
        books_by_status = {
            "processing": await books_collection.count_documents({"status": "processing"}),
            "complete": await books_collection.count_documents({"status": "complete"})
        }
        
        # Users by role
        users_by_role = {
            "admin": await users_collection.count_documents({"role": "admin"}),
            "user": await users_collection.count_documents({"role": "user"})
        }
        
        return AdminStatsResponse(
            total_users=total_users,
            total_books=total_books,
            total_chats=total_chats,
            active_users_30d=active_users_30d,
            books_by_status=books_by_status,
            users_by_role=users_by_role
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching stats: {str(e)}")

# ==============================
# USER MANAGEMENT
# ==============================
@router.get("/users", response_model=PaginatedUsersResponse)
async def get_all_users_admin(
    admin_user: dict = Depends(get_admin_user),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000),
    search: Optional[str] = None,
    role: Optional[str] = None
):
    """Get all users with filtering and pagination."""
    try:
        # Build query
        query = {}
        if search:
            query["$or"] = [
                {"username": {"$regex": search, "$options": "i"}},
                {"email": {"$regex": search, "$options": "i"}},
                {"name": {"$regex": search, "$options": "i"}}
            ]
        if role:
            query["role"] = role
        
        # Get total count
        total = await users_collection.count_documents(query)
        
        # Get paginated results
        skip = (page - 1) * limit
        cursor = users_collection.find(query, {"_id": 0, "pass_hash": 0}).skip(skip).limit(limit)
        users = await cursor.to_list(length=limit)
        
        return PaginatedUsersResponse(
            users=users,
            total=total,
            page=page,
            limit=limit,
            has_more=(skip + limit) < total
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching users: {str(e)}")

@router.get("/users/{user_id}")
async def get_user_by_id_admin(user_id: str, admin_user: dict = Depends(get_admin_user)):
    """Get a specific user by ID."""
    user = await users_collection.find_one({"id": user_id}, {"_id": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    # Remove password hash for security
    user.pop("pass_hash", None)
    return user

@router.put("/users/{user_id}")
async def update_user_admin(
    user_id: str,
    updates: Dict[str, Any] = Body(...),
    admin_user: dict = Depends(get_admin_user)
):
    """Update any user field (admin only)."""
    user = await users_collection.find_one({"id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Hash password if it's being updated
    if "password" in updates:
        updates["pass_hash"] = hash_password(updates.pop("password"))
        updates["last_password_change"] = datetime.utcnow()
    
    # Prevent changing admin's own role (safety measure)
    if "role" in updates and str(admin_user["id"]) == user_id:
        if updates["role"] != "admin":
            raise HTTPException(status_code=400, detail="Cannot change your own admin role")
    
    # Update user
    await users_collection.update_one(
        {"id": user_id},
        {"$set": updates}
    )
    
    updated_user = await users_collection.find_one({"id": user_id}, {"_id": 0, "pass_hash": 0})
    return {"message": "User updated successfully", "user": updated_user}

@router.delete("/users/{user_id}")
async def delete_user_admin(user_id: str, admin_user: dict = Depends(get_admin_user)):
    """Delete a user and optionally their associated data."""
    user = await users_collection.find_one({"id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Prevent admin from deleting themselves
    if str(admin_user["id"]) == user_id:
        raise HTTPException(status_code=400, detail="Cannot delete your own account")
    
    # Delete user
    await users_collection.delete_one({"id": user_id})
    
    # Optionally delete user's chats and books
    # (You can add cascade delete logic here if needed)
    
    return {"message": "User deleted successfully"}

@router.post("/users/{user_id}/promote")
async def promote_user_to_admin(user_id: str, admin_user: dict = Depends(get_admin_user)):
    """Promote a user to admin role."""
    user = await users_collection.find_one({"id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user.get("role") == "admin":
        raise HTTPException(status_code=400, detail="User is already an admin")
    
    await users_collection.update_one(
        {"id": user_id},
        {"$set": {"role": "admin"}}
    )
    
    return {"message": "User promoted to admin successfully"}

@router.post("/users/{user_id}/demote")
async def demote_admin_to_user(user_id: str, admin_user: dict = Depends(get_admin_user)):
    """Demote an admin to user role."""
    user = await users_collection.find_one({"id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if user.get("role") != "admin":
        raise HTTPException(status_code=400, detail="User is not an admin")
    
    # Prevent admin from demoting themselves
    if str(admin_user["id"]) == user_id:
        raise HTTPException(status_code=400, detail="Cannot demote your own admin role")
    
    await users_collection.update_one(
        {"id": user_id},
        {"$set": {"role": "user"}}
    )
    
    return {"message": "Admin demoted to user successfully"}

# ==============================
# BOOK MANAGEMENT
# ==============================
@router.get("/books", response_model=PaginatedBooksResponse)
async def get_all_books_admin(
    admin_user: dict = Depends(get_admin_user),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000),
    search: Optional[str] = None,
    status: Optional[str] = None
):
    """Get all books with filtering and pagination."""
    try:
        query = {}
        if search:
            query["$or"] = [
                {"title": {"$regex": search, "$options": "i"}},
                {"author_name": {"$regex": search, "$options": "i"}}
            ]
        if status:
            query["status"] = status
        
        total = await books_collection.count_documents(query)
        skip = (page - 1) * limit
        cursor = books_collection.find(query, {"_id": 0}).skip(skip).limit(limit)
        books = await cursor.to_list(length=limit)
        
        return PaginatedBooksResponse(
            books=books,
            total=total,
            page=page,
            limit=limit,
            has_more=(skip + limit) < total
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching books: {str(e)}")

@router.get("/books/{book_id}")
async def get_book_by_id_admin(book_id: str, admin_user: dict = Depends(get_admin_user)):
    """Get a specific book by ID."""
    book = await books_collection.find_one({"id": book_id}, {"_id": 0})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return book

@router.put("/books/{book_id}")
async def update_book_admin(
    book_id: str,
    updates: Dict[str, Any] = Body(...),
    admin_user: dict = Depends(get_admin_user)
):
    """Update any book field."""
    book = await books_collection.find_one({"id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    await books_collection.update_one(
        {"id": book_id},
        {"$set": updates}
    )
    
    updated_book = await books_collection.find_one({"id": book_id}, {"_id": 0})
    return {"message": "Book updated successfully", "book": updated_book}

@router.delete("/books/{book_id}")
async def delete_book_admin(book_id: str, admin_user: dict = Depends(get_admin_user)):
    """Delete a book."""
    book = await books_collection.find_one({"id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    await books_collection.delete_one({"id": book_id})
    
    # Optionally delete associated Qdrant vectors and B2 files
    # (You can add cleanup logic here)
    
    return {"message": "Book deleted successfully"}

# ==============================
# CHAT MANAGEMENT
# ==============================
@router.get("/chats", response_model=PaginatedChatsResponse)
async def get_all_chats_admin(
    admin_user: dict = Depends(get_admin_user),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=1000),
    user_id: Optional[str] = None,
    search: Optional[str] = None
):
    """Get all chat sessions with filtering and pagination."""
    try:
        query = {}
        if user_id:
            query["user_id"] = user_id
        if search:
            query["$or"] = [
                {"title": {"$regex": search, "$options": "i"}},
                {"messages.content": {"$regex": search, "$options": "i"}}
            ]
        
        total = await chat_sessions_collection.count_documents(query)
        skip = (page - 1) * limit
        cursor = chat_sessions_collection.find(query, {"_id": 0}).skip(skip).limit(limit)
        chats = await cursor.to_list(length=limit)
        
        return PaginatedChatsResponse(
            chats=chats,
            total=total,
            page=page,
            limit=limit,
            has_more=(skip + limit) < total
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching chats: {str(e)}")

@router.get("/chats/{chat_id}")
async def get_chat_by_id_admin(chat_id: str, admin_user: dict = Depends(get_admin_user)):
    """Get a specific chat session by ID."""
    chat = await chat_sessions_collection.find_one({"id": chat_id}, {"_id": 0})
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    return chat

@router.put("/chats/{chat_id}")
async def update_chat_admin(
    chat_id: str,
    updates: Dict[str, Any] = Body(...),
    admin_user: dict = Depends(get_admin_user)
):
    """Update any chat field."""
    chat = await chat_sessions_collection.find_one({"id": chat_id})
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    await chat_sessions_collection.update_one(
        {"id": chat_id},
        {"$set": updates}
    )
    
    updated_chat = await chat_sessions_collection.find_one({"id": chat_id}, {"_id": 0})
    return {"message": "Chat updated successfully", "chat": updated_chat}

@router.delete("/chats/{chat_id}")
async def delete_chat_admin(chat_id: str, admin_user: dict = Depends(get_admin_user)):
    """Delete a chat session."""
    chat = await chat_sessions_collection.find_one({"id": chat_id})
    if not chat:
        raise HTTPException(status_code=404, detail="Chat not found")
    
    await chat_sessions_collection.delete_one({"id": chat_id})
    return {"message": "Chat deleted successfully"}

# ==============================
# COLLECTION MANAGEMENT (Generic)
# ==============================
@router.post("/collections/query")
async def query_collection(
    request: CollectionQueryRequest,
    admin_user: dict = Depends(get_admin_user)
):
    """Generic query endpoint for any collection."""
    allowed_collections = ["users", "books", "chats", "pending_signups"]
    
    if request.collection not in allowed_collections:
        raise HTTPException(
            status_code=400,
            detail=f"Collection '{request.collection}' not allowed. Allowed: {allowed_collections}"
        )
    
    collection = db[request.collection]
    filters = request.filters or {}
    sort = request.sort or {}
    limit = min(request.limit or 100, 1000)
    skip = request.skip or 0
    
    try:
        total = await collection.count_documents(filters)
        cursor = collection.find(filters, {"_id": 0}).skip(skip).limit(limit)
        
        if sort:
            cursor = cursor.sort(list(sort.items()))
        
        results = await cursor.to_list(length=limit)
        
        return {
            "collection": request.collection,
            "results": results,
            "total": total,
            "skip": skip,
            "limit": limit,
            "has_more": (skip + limit) < total
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying collection: {str(e)}")

@router.post("/collections/{collection_name}/update")
async def update_collection_documents(
    collection_name: str,
    filters: Dict[str, Any] = Body(...),
    updates: Dict[str, Any] = Body(...),
    admin_user: dict = Depends(get_admin_user)
):
    """Bulk update documents in a collection."""
    allowed_collections = ["users", "books", "chats", "pending_signups"]
    
    if collection_name not in allowed_collections:
        raise HTTPException(
            status_code=400,
            detail=f"Collection '{collection_name}' not allowed"
        )
    
    collection = db[collection_name]
    
    try:
        result = await collection.update_many(
            filters,
            {"$set": updates}
        )
        
        return {
            "message": "Documents updated successfully",
            "matched_count": result.matched_count,
            "modified_count": result.modified_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error updating collection: {str(e)}")

@router.post("/collections/{collection_name}/delete")
async def delete_collection_documents(
    collection_name: str,
    filters: Dict[str, Any] = Body(...),
    admin_user: dict = Depends(get_admin_user)
):
    """Bulk delete documents from a collection."""
    allowed_collections = ["users", "books", "chats", "pending_signups"]
    
    if collection_name not in allowed_collections:
        raise HTTPException(
            status_code=400,
            detail=f"Collection '{collection_name}' not allowed"
        )
    
    # Prevent deleting all documents (safety check)
    if not filters or filters == {}:
        raise HTTPException(
            status_code=400,
            detail="Cannot delete all documents. Filters are required."
        )
    
    collection = db[collection_name]
    
    try:
        result = await collection.delete_many(filters)
        
        return {
            "message": "Documents deleted successfully",
            "deleted_count": result.deleted_count
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting from collection: {str(e)}")

# ==============================
# DATABASE OPERATIONS
# ==============================
@router.get("/collections/list")
async def list_all_collections(admin_user: dict = Depends(get_admin_user)):
    """List all collections in the database."""
    try:
        collections = await db.list_collection_names()
        collection_info = []
        
        for coll_name in collections:
            collection = db[coll_name]
            count = await collection.count_documents({})
            collection_info.append({
                "name": coll_name,
                "document_count": count
            })
        
        return {"collections": collection_info}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing collections: {str(e)}")

@router.get("/collections/{collection_name}/count")
async def get_collection_count(
    collection_name: str,
    filters: Optional[Dict[str, Any]] = None,
    admin_user: dict = Depends(get_admin_user)
):
    """Get document count for a collection."""
    try:
        collection = db[collection_name]
        count = await collection.count_documents(filters or {})
        return {"collection": collection_name, "count": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error counting documents: {str(e)}")

