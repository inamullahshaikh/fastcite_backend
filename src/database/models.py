from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List, Literal, Dict
from uuid import UUID, uuid4
from datetime import date, datetime


# ============================
# USER MODEL
# ============================
class User(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    username: str
    pass_hash: Optional[str]
    name: str
    dob: Optional[date] = None
    email: EmailStr
    role: Literal["user", "admin"] = "user"  # restricts values to 'user' or 'admin'

    class Config:
        orm_mode = True
        json_schema_extra = {
            "example": {
                "username": "inam123",
                "pass_hash": "hashed_password_here",
                "name": "Inam Ullah",
                "dob": "2002-04-10",
                "email": "inam@example.com",
                "role": "user"
            }
        }

    def to_mongo(self):
        """Convert UUIDs to strings for MongoDB insertion."""
        data = self.dict()
        data["id"] = str(self.id)
        return data


# ============================
# CHAT MESSAGE MODEL
# ============================
class ChatMessage(BaseModel):
    question: str
    answer: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_schema_extra = {
            "example": {
                "question": "What is deep learning?",
                "answer": "Deep learning is a subset of machine learning using neural networks.",
                "timestamp": "2025-10-30T12:00:00Z"
            }
        }

    def to_mongo(self):
        data = self.dict()
        data["timestamp"] = data["timestamp"].isoformat()
        return data


# ============================
# CHAT SESSION MODEL
# ============================
class ChatSession(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    user_id: Optional[str] = ""
    title: Optional[str] = Field(default="New Chat")
    messages: List[ChatMessage] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "740729b6-aa3c-47f1-a043-4425ed5d70eb",
                "title": "LLM Project Discussion",
                "messages": [
                    {
                        "question": "Explain retrieval-augmented generation.",
                        "answer": "RAG combines retrieval from a vector DB with text generation."
                    },
                    {
                        "question": "How can I integrate it with FastAPI?",
                        "answer": "Use a vector DB like Qdrant and query it inside a FastAPI route."
                    }
                ],
                "created_at": "2025-10-30T10:00:00Z",
                "updated_at": "2025-10-30T11:00:00Z"
            }
        }

    def to_mongo(self):
        """Convert UUIDs and datetime to strings for MongoDB."""
        data = self.dict()
        data["id"] = str(self.id)
        data["user_id"] = str(self.user_id)
        data["created_at"] = data["created_at"].isoformat()
        data["updated_at"] = data["updated_at"].isoformat()
        data["messages"] = [m.to_mongo() for m in self.messages]
        return data


# ============================
# BOOK MODEL
# ============================
class Book(BaseModel):
    id: UUID = Field(default_factory=uuid4)
    title: str
    author_name: Optional[str] = None
    pages: Optional[int] = None
    status: Literal["processing", "complete"]
    uploaded_at: datetime = Field(default_factory=datetime.utcnow)
    uploaded_by: Dict[str, str] = Field(default_factory=dict)  # {user_id: book_name}

    class Config:
        json_schema_extra = {
            "example": {
                "title": "Deep Learning with Python",
                "author_name": "François Chollet",
                "pages": 360,
                "status": "processing",
                "uploaded_at": "2025-10-30T12:00:00Z",
                "uploaded_by": {"user-id-123": "My Custom Book Name"}
            }
        }

    def to_mongo(self):
        """Convert UUIDs and datetime to strings for MongoDB."""
        data = self.dict()
        data["id"] = str(self.id)
        data["uploaded_at"] = data["uploaded_at"].isoformat()
        return data
