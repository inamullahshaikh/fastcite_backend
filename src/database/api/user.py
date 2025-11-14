from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr
from typing import List, Optional, Literal
from uuid import UUID
from datetime import datetime, date
from database.auth import get_current_user, users_collection, create_access_token
from database.models import User
router = APIRouter(prefix="/users", tags=["Users"])

# ----------------------------
# USER PROFILE MODEL (for frontend)
# ----------------------------
class UserProfile(BaseModel):
    id: UUID
    username: str
    name: str
    dob: Optional[str] = None  # ISO string
    email: EmailStr
    role: Literal["user", "admin"] = "user"

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "id": "3906100a-c589-4a10-be1a-c6d230533bf2",
                "username": "inam123",
                "name": "Inam Ullah",
                "dob": "2004-06-17",
                "email": "inam@example.com",
                "role": "user"
            }
        }

# ----------------------------
# CREATE USER (public)
# ----------------------------
@router.post("/", status_code=201)
async def create_user(user: User):
    existing_user = await users_collection.find_one({"email": user.email})
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already registered")

    data = user.dict()
    data["id"] = str(data["id"])  # ensure stored as string
    data["created_at"] = datetime.utcnow()
    await users_collection.insert_one(data)
    return {"message": "User created successfully"}

# ----------------------------
# GET ALL USERS (admin only)
# ----------------------------
@router.get("/", response_model=List[User])
async def get_all_users(current_user: dict = Depends(get_current_user)):
    if str(current_user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    
    cursor = users_collection.find({}, {"_id": 0})
    users = await cursor.to_list(length=1000)
    return users

# ----------------------------
# GET USER BY ID (admin or self)
# ----------------------------
@router.get("/{user_id}", response_model=User)
async def get_user_by_id(user_id: str, current_user: dict = Depends(get_current_user)):
    user = await users_collection.find_one({"id": user_id}, {"_id": 0})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # allow access if admin or self
    if str(current_user["id"]) != str(user_id) and str(current_user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Access denied")

    return user

# ----------------------------
# GET MY PROFILE
# ----------------------------
@router.get("/getmyprofile/me", response_model=UserProfile)
async def get_my_profile(current_user: dict = Depends(get_current_user)):
    dob_value = current_user.get("dob")
    dob = dob_value.isoformat() if isinstance(dob_value, (datetime, date)) else dob_value

    return {
        "id": str(current_user.get("id") or current_user.get("_id")),
        "username": current_user.get("username"),
        "name": current_user.get("name"),
        "email": current_user.get("email"),
        "role": current_user.get("role", "user"),
        "dob": dob,
    }

# ----------------------------
# UPDATE USER (admin or self)
# ----------------------------
@router.put("/{user_id}")
async def update_user(
    user_id: str,
    updates: dict,
    current_user: dict = Depends(get_current_user)
):
    # 1. Fetch user
    user = await users_collection.find_one({"id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # 2. Permission check
    if str(current_user["id"]) != str(user_id) and str(current_user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Access denied")

    # 3. Protect sensitive fields
    protected_fields = {"id", "email", "role"}
    updates = {k: v for k, v in updates.items() if k not in protected_fields}

    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    # 4. Apply updates
    await users_collection.update_one({"id": user_id}, {"$set": updates})

    # 5. Fetch updated user
    updated_user = await users_collection.find_one({"id": user_id})

    # 6. Create new token (always generate on update)
    token_data = {
        "sub": updated_user["username"],
        "role": updated_user["role"],
        "id": updated_user["id"],
    }
    new_access_token = create_access_token(token_data)

    # 7. Return response
    return {
        "message": "User updated successfully",
        "access_token": new_access_token,
    }

# ----------------------------
# DELETE USER (admin or self)
# ----------------------------
@router.delete("/{user_id}")
async def delete_user(user_id: str, current_user: dict = Depends(get_current_user)):
    user = await users_collection.find_one({"id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    if str(current_user["id"]) != str(user_id) and str(current_user.get("role")) != "admin":
        raise HTTPException(status_code=403, detail="Access denied")

    await users_collection.delete_one({"id": user_id})
    return {"message": "User deleted successfully"}
