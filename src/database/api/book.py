from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from pydantic import BaseModel
from database.auth import get_current_user
from database.mongo import books_collection
from database.models import Book

router = APIRouter(prefix="/books", tags=["Books"])

# ----------------------------
# CREATE BOOK (any logged user)
# ----------------------------
@router.post("/", status_code=201)
async def upload_book(book: Book, current_user: dict = Depends(get_current_user)):
    data = book.dict()
    data["uploader_id"] = str(current_user["id"])
    await books_collection.insert_one(data)  # async insert
    return {"message": "Book uploaded successfully"}

# ----------------------------
# GET ALL (admin)
# ----------------------------
@router.get("/", response_model=List[Book])
async def get_all_books(current_user: dict = Depends(get_current_user)):
    if current_user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    cursor = books_collection.find({}, {"_id": 0})
    books = await cursor.to_list(length=1000)  # convert async cursor to list
    return books

# ----------------------------
# GET MY BOOKS (self) - with pagination
# ----------------------------
class PaginatedBooksResponse(BaseModel):
    books: List[Book]
    total: int
    page: int
    limit: int
    has_more: bool

@router.get("/me", response_model=PaginatedBooksResponse)
async def get_my_books(
    current_user: dict = Depends(get_current_user),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    limit: int = Query(20, ge=1, le=100, description="Items per page (max 100)")
):
    user_id = str(current_user["id"])
    
    # Calculate skip
    skip = (page - 1) * limit
    
    # Query books where user_id is a key in uploaded_by dict
    # This handles both old list format and new dict format
    query = {
        "$or": [
            {f"uploaded_by.{user_id}": {"$exists": True}},  # New dict format
            {"uploaded_by": user_id}  # Old list format (for migration)
        ]
    }
    
    # Get total count
    total = await books_collection.count_documents(query)
    
    # Get paginated results, sorted by uploaded_at descending
    cursor = books_collection.find(
        query,
        {"_id": 0}
    ).sort("uploaded_at", -1).skip(skip).limit(limit)
    
    raw_books = await cursor.to_list(length=limit)

    books = []
    for b in raw_books:
        # Get user's custom book name from uploaded_by dict
        uploaded_by = b.get("uploaded_by", {})
        
        # Handle migration: if uploaded_by is a list, convert to dict
        if isinstance(uploaded_by, list):
            uploaded_by = {uid: b.get("title", "Untitled") for uid in uploaded_by}
        
        # Clean up uploaded_by dict: ensure all values are strings
        cleaned_uploaded_by = {}
        for uid, book_name in uploaded_by.items():
            if not isinstance(book_name, str):
                if isinstance(book_name, dict):
                    book_name = str(book_name.get("title", book_name.get("name", "Untitled")))
                else:
                    book_name = str(book_name) if book_name else "Untitled"
            cleaned_uploaded_by[str(uid)] = book_name.strip() if book_name else "Untitled"
        
        # Get user's book name (their custom name for this book)
        user_book_name = cleaned_uploaded_by.get(user_id)
        
        # Use user's book name if available, otherwise use PDF title
        display_title = user_book_name or b.get("title", "") or "Untitled"
        
        # Ensure display_title is always a string
        if not isinstance(display_title, str):
            if isinstance(display_title, dict):
                display_title = str(display_title.get("title", display_title.get("name", "Untitled")))
            else:
                display_title = str(display_title) if display_title else "Untitled"
        
        books.append(
            Book(
                id=b.get("id"),
                title=display_title,  # User's custom name or PDF title
                author_name=b.get("author_name"),
                pages=b.get("pages"),
                status=b.get("status", "processing"),
                uploaded_at=b.get("uploaded_at"),
                uploaded_by=cleaned_uploaded_by,  # Return cleaned dict
            )
        )

    return PaginatedBooksResponse(
        books=books,
        total=total,
        page=page,
        limit=limit,
        has_more=(skip + limit) < total
    )

# ----------------------------
# UPDATE BOOK (admin or uploader)
# ----------------------------
@router.put("/{book_id}")
async def update_book(book_id: str, updates: dict, current_user: dict = Depends(get_current_user)):
    book = await books_collection.find_one({"id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    if current_user["role"] != "admin" and book["uploader_id"] != str(current_user["id"]):
        raise HTTPException(status_code=403, detail="Access denied")

    await books_collection.update_one({"id": book_id}, {"$set": updates})
    return {"message": "Book updated successfully"}

# ----------------------------
# UPDATE MY BOOK NAME (self)
# ----------------------------
class UpdateBookNameRequest(BaseModel):
    book_name: str

@router.patch("/{book_id}/my-name")
async def update_my_book_name(
    book_id: str, 
    request: UpdateBookNameRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Update the current user's custom book name in the uploaded_by dict.
    
    Args:
        book_id: ID of the book
        request: Request body with book_name field
    """
    user_id = str(current_user["id"])
    book_name = request.book_name
    
    # Find the book
    book = await books_collection.find_one({"id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    # Check if user has access to this book
    uploaded_by = book.get("uploaded_by", {})
    
    # Handle migration: if uploaded_by is a list, convert to dict
    if isinstance(uploaded_by, list):
        uploaded_by = {uid: book.get("title", "Untitled") for uid in uploaded_by}
        await books_collection.update_one(
            {"id": book_id},
            {"$set": {"uploaded_by": uploaded_by}}
        )
    
    # Check if user is in uploaded_by
    if user_id not in uploaded_by:
        raise HTTPException(status_code=403, detail="You don't have access to this book")
    
    # Ensure book_name is a string
    if not isinstance(book_name, str):
        book_name = str(book_name) if book_name else "Untitled"
    book_name = book_name.strip() if book_name else "Untitled"
    
    # Update user's book name in uploaded_by dict
    await books_collection.update_one(
        {"id": book_id},
        {"$set": {f"uploaded_by.{user_id}": book_name}}
    )
    
    return {
        "message": "Book name updated successfully",
        "book_id": book_id,
        "book_name": book_name
    }

# ----------------------------
# UPDATE MY BOOK NAME (self)
# ----------------------------
@router.patch("/{book_id}/my-name")
async def update_my_book_name(book_id: str, book_name: str, current_user: dict = Depends(get_current_user)):
    """
    Update the current user's custom book name in the uploaded_by dict.
    
    Args:
        book_id: ID of the book
        book_name: New book name for this user
    """
    user_id = str(current_user["id"])
    
    # Find the book
    book = await books_collection.find_one({"id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    
    # Check if user has access to this book
    uploaded_by = book.get("uploaded_by", {})
    
    # Handle migration: if uploaded_by is a list, convert to dict
    if isinstance(uploaded_by, list):
        uploaded_by = {uid: book.get("title", "Untitled") for uid in uploaded_by}
        await books_collection.update_one(
            {"id": book_id},
            {"$set": {"uploaded_by": uploaded_by}}
        )
    
    # Check if user is in uploaded_by
    if user_id not in uploaded_by:
        raise HTTPException(status_code=403, detail="You don't have access to this book")
    
    # Ensure book_name is a string
    if not isinstance(book_name, str):
        book_name = str(book_name) if book_name else "Untitled"
    book_name = book_name.strip() if book_name else "Untitled"
    
    # Update user's book name in uploaded_by dict
    uploaded_by[user_id] = book_name
    
    await books_collection.update_one(
        {"id": book_id},
        {"$set": {f"uploaded_by.{user_id}": book_name}}
    )
    
    return {
        "message": "Book name updated successfully",
        "book_id": book_id,
        "book_name": book_name
    }

# ----------------------------
# DELETE BOOK (admin or uploader)
# ----------------------------
@router.delete("/{book_id}")
async def delete_book(book_id: str, current_user: dict = Depends(get_current_user)):
    book = await books_collection.find_one({"id": book_id})
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    if current_user["role"] != "admin" and book["uploader_id"] != str(current_user["id"]):
        raise HTTPException(status_code=403, detail="Access denied")

    await books_collection.delete_one({"id": book_id})
    return {"message": "Book deleted successfully"}
