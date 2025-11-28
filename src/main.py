from fastapi import FastAPI
from starlette.middleware.sessions import SessionMiddleware
from fastapi.middleware.cors import CORSMiddleware
import os
from pathlib import Path
from database.auth import router as auth_router
from database.api.user import router as user_router
from database.auth import router as auth_router
from database.api.user import router as user_router
from database.api.chat import router as chat_router
from database.api.book import router as book_router
from database.api.admin import router as admin_router
from app.books.routes import router as app_book_router
from app.rag.routes import router as rag_router
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="AI Project API", version="1.0")


BASE_DIR = Path(__file__).resolve().parent
DOWNLOADS_DIR = BASE_DIR / "downloads"
app.mount("/downloads", StaticFiles(directory=DOWNLOADS_DIR), name="downloads")
# ✅ Allow frontend (Vite) to access backend API
origins = [
    "http://localhost:5173",   # Vite dev server
    "http://127.0.0.1:5173",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Add session middleware
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET_KEY", "supersecret"),  # replace with strong secret in production
)

# ✅ Include routers
app.include_router(auth_router)
app.include_router(user_router)
app.include_router(chat_router)
app.include_router(book_router)
app.include_router(admin_router)  # Admin routes (requires admin role)
app.include_router(app_book_router)
app.include_router(rag_router)
# ✅ Root endpoint
@app.get("/")
async def root():
    return {"message": "Welcome to the AI Project API"}
