from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response, JSONResponse, StreamingResponse
from starlette.requests import Request
from pydantic import BaseModel, EmailStr
from typing import List, Optional, Literal
from uuid import UUID
from datetime import datetime, date
import pyotp
import qrcode
import io
import base64
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib.enums import TA_CENTER
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie
from collections import defaultdict
from database.auth import get_current_user, users_collection, create_access_token, hash_password, verify_password
from database.models import User
from database.mongo import books_collection, chat_sessions_collection
from services.email_service import email_service
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
# GET MY PROFILE (must be before /{user_id})
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
# CHANGE PASSWORD
# ----------------------------
class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str

@router.put("/changepassword")
async def change_password(
    request: ChangePasswordRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    Change password for the authenticated user.
    Requires old password verification and new password.
    """
    user_id = str(current_user["id"])
    
    # 1. Fetch user from database
    user = await users_collection.find_one({"id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # 2. Verify old password
    stored_password_hash = user.get("pass_hash")
    if not stored_password_hash:
        raise HTTPException(status_code=400, detail="Password not set for this account")
    
    if not verify_password(request.old_password, stored_password_hash):
        raise HTTPException(status_code=401, detail="Incorrect old password")
    
    # 3. Validate new password
    if not request.new_password or len(request.new_password) < 8:
        raise HTTPException(
            status_code=400, 
            detail="New password must be at least 8 characters long"
        )
    
    # 4. Check if new password is different from old password
    if verify_password(request.new_password, stored_password_hash):
        raise HTTPException(
            status_code=400, 
            detail="New password must be different from the old password"
        )
    
    # 5. Hash and update the new password
    new_password_hash = hash_password(request.new_password)
    await users_collection.update_one(
        {"id": user_id},
        {"$set": {
            "pass_hash": new_password_hash,
            "last_password_change": datetime.utcnow()
        }}
    )
    
    # 6. Send password change notification email
    try:
        email_service.send_password_changed_email(
            user_email=user.get("email"),
            user_name=user.get("name", user.get("username", "User"))
        )
    except Exception as e:
        print(f"⚠️ Failed to send password change email: {e}")
    
    # 7. Return success message
    return {"message": "Password changed successfully"}

# ----------------------------
# TWO-FACTOR AUTHENTICATION (must be before /{user_id})
# ----------------------------
class Enable2FARequest(BaseModel):
    code: str  # TOTP code to verify

class Verify2FACodeRequest(BaseModel):
    code: str

@router.get("/2fa/status")
async def get_2fa_status(current_user: dict = Depends(get_current_user)):
    """Check if 2FA is enabled for the current user."""
    user = await users_collection.find_one({"id": str(current_user["id"])})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    is_enabled = bool(user.get("two_factor_secret") and user.get("two_factor_enabled", False))
    return {"enabled": is_enabled}

@router.post("/2fa/generate")
async def generate_2fa_secret(current_user: dict = Depends(get_current_user)):
    """
    Generate a new 2FA secret and QR code.
    Returns the secret and QR code image as base64.
    """
    user = await users_collection.find_one({"id": str(current_user["id"])})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Generate a new secret
    secret = pyotp.random_base32()
    
    # Create TOTP URI
    totp_uri = pyotp.totp.TOTP(secret).provisioning_uri(
        name=user.get("email", user.get("username", "User")),
        issuer_name="FastCite"
    )
    
    # Generate QR code
    qr = qrcode.QRCode(version=1, box_size=10, border=5)
    qr.add_data(totp_uri)
    qr.make(fit=True)
    
    img = qr.make_image(fill_color="black", back_color="white")
    
    # Convert to base64
    buffer = io.BytesIO()
    img.save(buffer, format="PNG")
    buffer.seek(0)
    qr_code_base64 = base64.b64encode(buffer.read()).decode()
    
    # Temporarily store secret (not enabled yet - user must verify code first)
    await users_collection.update_one(
        {"id": str(current_user["id"])},
        {"$set": {"two_factor_secret": secret, "two_factor_enabled": False}}
    )
    
    return {
        "secret": secret,
        "qr_code": f"data:image/png;base64,{qr_code_base64}",
        "manual_entry_key": secret
    }

@router.post("/2fa/enable")
async def enable_2fa(request: Enable2FARequest, current_user: dict = Depends(get_current_user)):
    """
    Enable 2FA by verifying the TOTP code.
    The secret must have been generated first via /2fa/generate.
    """
    user = await users_collection.find_one({"id": str(current_user["id"])})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    secret = user.get("two_factor_secret")
    if not secret:
        raise HTTPException(status_code=400, detail="No 2FA secret found. Please generate one first.")
    
    # Verify the code
    totp = pyotp.TOTP(secret)
    if not totp.verify(request.code, valid_window=1):
        raise HTTPException(status_code=400, detail="Invalid verification code. Please try again.")
    
    # Enable 2FA
    await users_collection.update_one(
        {"id": str(current_user["id"])},
        {"$set": {"two_factor_enabled": True}}
    )
    
    return {"message": "2FA enabled successfully"}

@router.post("/2fa/disable")
async def disable_2fa(request: Verify2FACodeRequest, current_user: dict = Depends(get_current_user)):
    """
    Disable 2FA by verifying the TOTP code.
    """
    user = await users_collection.find_one({"id": str(current_user["id"])})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    if not user.get("two_factor_enabled"):
        raise HTTPException(status_code=400, detail="2FA is not enabled for this account")
    
    secret = user.get("two_factor_secret")
    if not secret:
        raise HTTPException(status_code=400, detail="2FA secret not found")
    
    # Verify the code
    totp = pyotp.TOTP(secret)
    if not totp.verify(request.code, valid_window=1):
        raise HTTPException(status_code=400, detail="Invalid verification code. Please try again.")
    
    # Disable 2FA and remove secret
    await users_collection.update_one(
        {"id": str(current_user["id"])},
        {"$set": {"two_factor_enabled": False}, "$unset": {"two_factor_secret": ""}}
    )
    
    return {"message": "2FA disabled successfully"}

# ----------------------------
# USER SESSIONS & SECURITY
# ----------------------------
class SessionInfo(BaseModel):
    device: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    last_activity: Optional[datetime] = None
    created_at: Optional[datetime] = None

@router.get("/sessions")
async def get_active_sessions(current_user: dict = Depends(get_current_user)):
    """Get all active sessions for the current user."""
    user = await users_collection.find_one({"id": str(current_user["id"])})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    sessions = user.get("active_sessions", [])
    return {"sessions": sessions}

@router.post("/sessions/logout-all")
async def logout_all_devices(
    current_user: dict = Depends(get_current_user)
):
    """
    Sign out from all devices except the current one.
    This invalidates all tokens by updating the token_version.
    """
    user = await users_collection.find_one({"id": str(current_user["id"])})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Increment token_version to invalidate all existing tokens
    current_version = user.get("token_version", 0)
    await users_collection.update_one(
        {"id": str(current_user["id"])},
        {
            "$set": {
                "token_version": current_version + 1,
                "active_sessions": []
            }
        }
    )
    
    return {"message": "Signed out from all devices successfully"}

@router.get("/security-info")
async def get_security_info(current_user: dict = Depends(get_current_user)):
    """Get security information including last password change and last login."""
    user = await users_collection.find_one({"id": str(current_user["id"])})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {
        "last_password_change": user.get("last_password_change"),
        "last_login": user.get("last_login"),
        "account_created": user.get("created_at")
    }

# ----------------------------
# USER PREFERENCES
# ----------------------------
class UserPreferences(BaseModel):
    theme: Optional[Literal["light", "dark"]] = None
    notifications: Optional[dict] = None
    language: Optional[str] = None
    timezone: Optional[str] = None
    date_format: Optional[str] = None

@router.get("/preferences")
async def get_user_preferences(current_user: dict = Depends(get_current_user)):
    """Get user preferences."""
    user = await users_collection.find_one({"id": str(current_user["id"])})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    preferences = user.get("preferences", {})
    return {
        "theme": preferences.get("theme", "dark"),
        "notifications": preferences.get("notifications", {
            "email": True,
            "push": False,
            "updates": True
        }),
        "language": preferences.get("language", "en"),
        "timezone": preferences.get("timezone", "UTC"),
        "date_format": preferences.get("date_format", "MM/DD/YYYY")
    }

@router.put("/preferences")
async def update_user_preferences(
    preferences: UserPreferences,
    current_user: dict = Depends(get_current_user)
):
    """Update user preferences."""
    user = await users_collection.find_one({"id": str(current_user["id"])})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    current_prefs = user.get("preferences", {})
    updates = {}
    
    if preferences.theme is not None:
        updates["theme"] = preferences.theme
    if preferences.notifications is not None:
        updates["notifications"] = preferences.notifications
    if preferences.language is not None:
        updates["language"] = preferences.language
    if preferences.timezone is not None:
        updates["timezone"] = preferences.timezone
    if preferences.date_format is not None:
        updates["date_format"] = preferences.date_format
    
    current_prefs.update(updates)
    
    await users_collection.update_one(
        {"id": str(current_user["id"])},
        {"$set": {"preferences": current_prefs}}
    )
    
    return {"message": "Preferences updated successfully", "preferences": current_prefs}

# ----------------------------
# DOWNLOAD USER DATA
# ----------------------------
@router.get("/download-data")
async def download_user_data(current_user: dict = Depends(get_current_user)):
    """Download all user data as a formatted PDF."""
    import os
    from pathlib import Path
    
    user = await users_collection.find_one({"id": str(current_user["id"])})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    user_id = str(current_user["id"])
    
    # Fetch user's books
    books_cursor = books_collection.find({"uploaded_by": {"$exists": True}})
    all_books = await books_cursor.to_list(length=1000)
    user_books = [
        book for book in all_books 
        if isinstance(book.get("uploaded_by"), dict) and user_id in book.get("uploaded_by", {})
    ]
    
    # Fetch user's chat sessions
    chats_cursor = chat_sessions_collection.find({"user_id": user_id})
    user_chats = await chats_cursor.to_list(length=1000)
    
    # Calculate analytics and prepare data for charts
    def parse_datetime(value):
        """Parse datetime from various formats."""
        if isinstance(value, datetime):
            return value
        elif isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
            except:
                try:
                    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S")
                except:
                    return None
        return None
    
    # Analytics calculations
    total_books = len(user_books)
    books_complete = sum(1 for b in user_books if b.get("status") == "complete")
    books_processing = sum(1 for b in user_books if b.get("status") == "processing")
    total_pages = sum(b.get("pages", 0) or 0 for b in user_books)
    total_chats = len(user_chats)
    total_messages = sum(len(chat.get("messages", [])) for chat in user_chats)
    
    # Book upload timeline (last 12 months)
    now = datetime.utcnow()
    months_data = defaultdict(int)
    for book in user_books:
        dt = parse_datetime(book.get("uploaded_at"))
        if dt:
            month_key = dt.strftime("%Y-%m")
            months_data[month_key] += 1
    
    # Chat activity timeline (last 12 months)
    chat_months_data = defaultdict(int)
    for chat in user_chats:
        dt = parse_datetime(chat.get("created_at"))
        if dt:
            month_key = dt.strftime("%Y-%m")
            chat_months_data[month_key] += 1
    
    # Activity by day of week
    day_of_week_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    day_activity = defaultdict(int)
    for book in user_books:
        dt = parse_datetime(book.get("uploaded_at"))
        if dt:
            day_activity[dt.weekday()] += 1
    for chat in user_chats:
        dt = parse_datetime(chat.get("created_at"))
        if dt:
            day_activity[dt.weekday()] += 1
    
    # Activity by hour
    hour_activity = defaultdict(int)
    for chat in user_chats:
        for msg in chat.get("messages", []):
            dt = parse_datetime(msg.get("timestamp"))
            if dt:
                hour_activity[dt.hour] += 1
    
    # Messages per chat distribution
    messages_per_chat = [len(chat.get("messages", [])) for chat in user_chats]
    
    # Helper function to format datetime
    def format_datetime(value):
        if isinstance(value, datetime):
            return value.strftime("%B %d, %Y at %I:%M %p")
        elif isinstance(value, date):
            return value.strftime("%B %d, %Y")
        elif isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                return dt.strftime("%B %d, %Y at %I:%M %p")
            except:
                return value
        return value or "N/A"
    
    # Create PDF in memory with better margins
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer, 
        pagesize=letter, 
        topMargin=0.4*inch, 
        bottomMargin=0.4*inch,
        leftMargin=0.5*inch,
        rightMargin=0.5*inch
    )
    story = []
    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'CustomTitle',
        parent=styles['Heading1'],
        fontSize=24,
        textColor=HexColor('#1e40af'),
        spaceAfter=30,
        alignment=TA_CENTER
    )
    
    heading_style = ParagraphStyle(
        'CustomHeading',
        parent=styles['Heading2'],
        fontSize=16,
        textColor=HexColor('#1e40af'),
        spaceAfter=12,
        spaceBefore=20
    )
    
    subheading_style = ParagraphStyle(
        'CustomSubheading',
        parent=styles['Heading3'],
        fontSize=14,
        textColor=HexColor('#3b82f6'),
        spaceAfter=8,
        spaceBefore=12
    )
    
    # Try to load logo - check multiple possible paths
    logo_path = None
    # Get the current file's directory and navigate to project root
    current_file = Path(__file__).resolve()  # /path/to/fastcite_backend/src/database/api/user.py
    # Go up: api -> database -> src -> fastcite_backend -> fastcite (project root)
    project_root = current_file.parent.parent.parent.parent.parent
    logo_paths = [
        project_root / "FastCite_frontend" / "public" / "logo.png",
        project_root / "FastCite_frontend" / "src" / "assets" / "logo.png",
        project_root / "fastcite_frontend" / "public" / "logo.png",
        project_root / "fastcite_frontend" / "src" / "assets" / "logo.png",
        # Also try relative paths from current working directory
        Path("FastCite_frontend/public/logo.png"),
        Path("FastCite_frontend/src/assets/logo.png"),
        Path("../FastCite_frontend/public/logo.png"),
        Path("../FastCite_frontend/src/assets/logo.png"),
        Path("../../FastCite_frontend/public/logo.png"),
        Path("../../FastCite_frontend/src/assets/logo.png"),
    ]
    
    for path in logo_paths:
        try:
            if path.exists():
                logo_path = str(path.resolve())
                print(f"✅ Found logo at: {logo_path}")
                break
        except Exception as e:
            continue
    
    if not logo_path:
        print(f"⚠️ Logo not found. Checked paths:")
        for path in logo_paths[:4]:  # Print first 4 paths
            print(f"   - {path} (exists: {path.exists()})")
    
    # Header with logo
    if logo_path:
        try:
            # Verify the file exists and is readable
            logo_file = Path(logo_path)
            if logo_file.exists() and logo_file.is_file():
                img = Image(str(logo_path), width=2*inch, height=2*inch)
                img.hAlign = 'CENTER'
                story.append(img)
                story.append(Spacer(1, 0.2*inch))
                print(f"✅ Logo added to PDF successfully")
            else:
                print(f"⚠️ Logo file not accessible: {logo_path}")
        except Exception as e:
            # Log error but continue without logo
            print(f"⚠️ Error loading logo from {logo_path}: {e}")
            import traceback
            traceback.print_exc()
    
    # Title with better styling
    story.append(Paragraph("User Data Export", title_style))
    subtitle_text = f"<i>Generated on {datetime.utcnow().strftime('%B %d, %Y at %I:%M %p')}</i>"
    story.append(Paragraph(subtitle_text, ParagraphStyle(
        'Subtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=HexColor('#64748b'),
        alignment=TA_CENTER,
        spaceAfter=20
    )))
    story.append(Spacer(1, 0.2*inch))
    
    # User Information Section
    story.append(Paragraph("Account Information", heading_style))
    user_info_data = [
        ['Field', 'Value'],
        ['Username', user.get("username", "N/A")],
        ['Full Name', user.get("name", "N/A")],
        ['Email', user.get("email", "N/A")],
        ['Role', user.get("role", "user").capitalize()],
        ['Date of Birth', format_datetime(user.get("dob"))],
        ['Account Created', format_datetime(user.get("created_at"))],
        ['Last Login', format_datetime(user.get("last_login"))],
        ['Last Password Change', format_datetime(user.get("last_password_change"))],
        ['Two-Factor Authentication', "Enabled" if user.get("two_factor_enabled") else "Disabled"],
    ]
    
    user_table = Table(user_info_data, colWidths=[2.5*inch, 4*inch])
    user_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), HexColor('#1e40af')),
        ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 12),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('BACKGROUND', (0, 1), (-1, -1), HexColor('#f8fafc')),
        ('GRID', (0, 0), (-1, -1), 1, HexColor('#e2e8f0')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [HexColor('#ffffff'), HexColor('#f8fafc')]),
    ]))
    story.append(user_table)
    story.append(Spacer(1, 0.3*inch))
    
    # Analytics Overview Section
    story.append(Paragraph("Analytics Overview", heading_style))
    analytics_data = [
        ['Metric', 'Value'],
        ['Total Books Uploaded', str(total_books)],
        ['Books Completed', str(books_complete)],
        ['Books Processing', str(books_processing)],
        ['Total Pages', f"{total_pages:,}" if total_pages > 0 else "0"],
        ['Average Pages per Book', f"{total_pages // total_books if total_books > 0 else 0:,}"],
        ['Total Chat Sessions', str(total_chats)],
        ['Total Messages', str(total_messages)],
        ['Average Messages per Chat', f"{total_messages // total_chats if total_chats > 0 else 0}"],
        ['Account Age', f"{(now - (parse_datetime(user.get('created_at')) or now)).days} days" if parse_datetime(user.get('created_at')) else "N/A"],
    ]
    
    # Calculate table width to fit page (7.5 inches available with margins)
    analytics_table = Table(analytics_data, colWidths=[3*inch, 4.5*inch])
    analytics_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), HexColor('#1e40af')),
        ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
        ('TOPPADDING', (0, 1), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
        ('LEFTPADDING', (0, 0), (-1, -1), 10),
        ('RIGHTPADDING', (0, 0), (-1, -1), 10),
        ('BACKGROUND', (0, 1), (-1, -1), HexColor('#f8fafc')),
        ('GRID', (0, 0), (-1, -1), 1, HexColor('#e2e8f0')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [HexColor('#ffffff'), HexColor('#f8fafc')]),
    ]))
    story.append(analytics_table)
    story.append(Spacer(1, 0.3*inch))
    
    # Book Status Distribution Pie Chart
    if total_books > 0:
        story.append(Paragraph("Book Status Distribution", heading_style))
        drawing = Drawing(6*inch, 3*inch)
        pie = Pie()
        pie.x = 1.5*inch
        pie.y = 0.5*inch
        pie.width = 3*inch
        pie.height = 2*inch
        pie.data = [books_complete, books_processing]
        pie.labels = ['Complete', 'Processing']
        pie.slices.strokeWidth = 0.5
        pie.slices[0].fillColor = HexColor('#10b981')  # Green for complete
        pie.slices[1].fillColor = HexColor('#f59e0b')  # Orange for processing
        drawing.add(pie)
        story.append(drawing)
        story.append(Spacer(1, 0.2*inch))
    
    # Book Uploads Over Time Chart
    if months_data:
        story.append(Paragraph("Book Uploads Over Time", heading_style))
        sorted_months = sorted(months_data.items())[-12:]  # Last 12 months
        if sorted_months:
            drawing = Drawing(6*inch, 3*inch)
            chart = VerticalBarChart()
            chart.x = 0.5*inch
            chart.y = 0.5*inch
            chart.width = 5*inch
            chart.height = 2*inch
            chart.data = [[months_data[month] for month, _ in sorted_months]]
            chart.categoryAxis.categoryNames = [month[-2:] for month, _ in sorted_months]  # Show only month number
            chart.bars[0].fillColor = HexColor('#3b82f6')
            chart.valueAxis.valueMin = 0
            chart.valueAxis.valueMax = max(months_data.values()) + 1 if months_data.values() else 1
            drawing.add(chart)
            story.append(drawing)
            story.append(Spacer(1, 0.2*inch))
    
    # Chat Activity Over Time Chart
    if chat_months_data:
        story.append(Paragraph("Chat Activity Over Time", heading_style))
        sorted_chat_months = sorted(chat_months_data.items())[-12:]  # Last 12 months
        if sorted_chat_months:
            drawing = Drawing(6*inch, 3*inch)
            chart = VerticalBarChart()
            chart.x = 0.5*inch
            chart.y = 0.5*inch
            chart.width = 5*inch
            chart.height = 2*inch
            chart.data = [[chat_months_data[month] for month, _ in sorted_chat_months]]
            chart.categoryAxis.categoryNames = [month[-2:] for month, _ in sorted_chat_months]
            chart.bars[0].fillColor = HexColor('#8b5cf6')
            chart.valueAxis.valueMin = 0
            chart.valueAxis.valueMax = max(chat_months_data.values()) + 1 if chat_months_data.values() else 1
            drawing.add(chart)
            story.append(drawing)
            story.append(Spacer(1, 0.2*inch))
    
    # Activity by Day of Week
    if day_activity:
        story.append(Paragraph("Activity by Day of Week", heading_style))
        day_values = [day_activity[i] for i in range(7)]
        drawing = Drawing(6*inch, 3*inch)
        chart = VerticalBarChart()
        chart.x = 0.5*inch
        chart.y = 0.5*inch
        chart.width = 5*inch
        chart.height = 2*inch
        chart.data = [day_values]
        chart.categoryAxis.categoryNames = [day_of_week_names[i][:3] for i in range(7)]
        chart.bars[0].fillColor = HexColor('#ec4899')
        chart.valueAxis.valueMin = 0
        chart.valueAxis.valueMax = max(day_values) + 1 if day_values else 1
        drawing.add(chart)
        story.append(drawing)
        story.append(Spacer(1, 0.2*inch))
    
    # Activity by Hour of Day
    if hour_activity:
        story.append(Paragraph("Chat Activity by Hour of Day", heading_style))
        hour_values = [hour_activity[i] for i in range(24)]
        drawing = Drawing(6*inch, 3*inch)
        chart = VerticalBarChart()
        chart.x = 0.5*inch
        chart.y = 0.5*inch
        chart.width = 5*inch
        chart.height = 2*inch
        chart.data = [hour_values]
        chart.categoryAxis.categoryNames = [str(i) for i in range(24)]
        chart.bars[0].fillColor = HexColor('#06b6d4')
        chart.valueAxis.valueMin = 0
        chart.valueAxis.valueMax = max(hour_values) + 1 if hour_values else 1
        drawing.add(chart)
        story.append(drawing)
        story.append(Spacer(1, 0.2*inch))
    
    # Messages per Chat Distribution
    if messages_per_chat:
        story.append(Paragraph("Messages per Chat Session", heading_style))
        # Create bins: 0, 1-5, 6-10, 11-20, 21-50, 50+
        bins = [0, 0, 0, 0, 0, 0]  # 0, 1-5, 6-10, 11-20, 21-50, 50+
        bin_labels = ['0', '1-5', '6-10', '11-20', '21-50', '50+']
        for count in messages_per_chat:
            if count == 0:
                bins[0] += 1
            elif count <= 5:
                bins[1] += 1
            elif count <= 10:
                bins[2] += 1
            elif count <= 20:
                bins[3] += 1
            elif count <= 50:
                bins[4] += 1
            else:
                bins[5] += 1
        
        drawing = Drawing(6*inch, 3*inch)
        chart = VerticalBarChart()
        chart.x = 0.5*inch
        chart.y = 0.5*inch
        chart.width = 5*inch
        chart.height = 2*inch
        chart.data = [bins]
        chart.categoryAxis.categoryNames = bin_labels
        chart.bars[0].fillColor = HexColor('#ef4444')
        chart.valueAxis.valueMin = 0
        chart.valueAxis.valueMax = max(bins) + 1 if bins else 1
        drawing.add(chart)
        story.append(drawing)
        story.append(Spacer(1, 0.3*inch))
    
    # Books Section - Detailed
    story.append(Paragraph("Uploaded Books - Detailed List", heading_style))
    if user_books:
        # Sort books by upload date (newest first)
        sorted_books = sorted(user_books, key=lambda x: parse_datetime(x.get("uploaded_at")) or datetime.min, reverse=True)
        
        story.append(Paragraph(f"Total Books: {len(user_books)} | Complete: {books_complete} | Processing: {books_processing}", subheading_style))
        books_data = [['#', 'Title', 'Author', 'Pages', 'Status', 'Uploaded']]
        for idx, book in enumerate(sorted_books, 1):
            book_name = book.get("uploaded_by", {}).get(user_id, book.get("title", "Untitled"))
            books_data.append([
                str(idx),
                book_name[:35] + "..." if len(book_name) > 35 else book_name,
                (book.get("author_name", "Unknown")[:25] or "Unknown")[:25],
                str(book.get("pages", "N/A")),
                book.get("status", "unknown").capitalize(),
                format_datetime(book.get("uploaded_at"))[:20] if format_datetime(book.get("uploaded_at")) != "N/A" else "N/A"
            ])
        
        books_table = Table(books_data, colWidths=[0.4*inch, 2*inch, 1.2*inch, 0.7*inch, 0.9*inch, 1.8*inch])
        books_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#1e40af')),
            ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),  # Center the # column
            ('ALIGN', (3, 0), (3, -1), 'CENTER'),  # Center pages
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
            ('TOPPADDING', (0, 1), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 5),
            ('BACKGROUND', (0, 1), (-1, -1), HexColor('#f8fafc')),
            ('GRID', (0, 0), (-1, -1), 1, HexColor('#e2e8f0')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [HexColor('#ffffff'), HexColor('#f8fafc')]),
        ]))
        story.append(books_table)
        
        # Book statistics
        if total_books > 0:
            story.append(Spacer(1, 0.2*inch))
            book_stats_text = f"<b>Book Statistics:</b> Average pages per book: {total_pages // total_books if total_books > 0 else 0:,} | "
            book_stats_text += f"Largest book: {max((b.get('pages', 0) or 0 for b in user_books), default=0):,} pages | "
            book_stats_text += f"Completion rate: {(books_complete / total_books * 100) if total_books > 0 else 0:.1f}%"
            story.append(Paragraph(book_stats_text, styles['Normal']))
    else:
        story.append(Paragraph("No books uploaded yet.", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # Chat Sessions Section - Detailed
    story.append(Paragraph("Chat Sessions - Detailed List", heading_style))
    if user_chats:
        # Sort chats by last updated (newest first)
        sorted_chats = sorted(user_chats, key=lambda x: parse_datetime(x.get("updated_at")) or datetime.min, reverse=True)
        
        story.append(Paragraph(f"Total Chat Sessions: {len(user_chats)} | Total Messages: {total_messages}", subheading_style))
        chats_data = [['#', 'Title', 'Messages', 'Created', 'Last Updated']]
        for idx, chat in enumerate(sorted_chats, 1):
            messages = chat.get("messages", [])
            chat_title = chat.get("title", "New Chat")
            chats_data.append([
                str(idx),
                chat_title[:45] + "..." if len(chat_title) > 45 else chat_title,
                str(len(messages)),
                format_datetime(chat.get("created_at"))[:20] if format_datetime(chat.get("created_at")) != "N/A" else "N/A",
                format_datetime(chat.get("updated_at"))[:20] if format_datetime(chat.get("updated_at")) != "N/A" else "N/A"
            ])
        
        # Adjust column widths to fit page (total ~7.5 inches)
        chats_table = Table(chats_data, colWidths=[0.35*inch, 3.5*inch, 0.7*inch, 1.2*inch, 1.15*inch])
        chats_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#1e40af')),
            ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
            ('ALIGN', (0, 0), (0, -1), 'CENTER'),  # Center the # column
            ('ALIGN', (2, 0), (2, -1), 'CENTER'),  # Center messages count
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),
            ('TOPPADDING', (0, 1), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('BACKGROUND', (0, 1), (-1, -1), HexColor('#f8fafc')),
            ('GRID', (0, 0), (-1, -1), 1, HexColor('#e2e8f0')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [HexColor('#ffffff'), HexColor('#f8fafc')]),
        ]))
        story.append(chats_table)
        
        # Chat statistics with better formatting
        if total_chats > 0:
            story.append(Spacer(1, 0.15*inch))
            avg_messages = total_messages // total_chats if total_chats > 0 else 0
            most_active = max((len(c.get('messages', [])) for c in user_chats), default=0)
            longest_chat = max(messages_per_chat) if messages_per_chat else 0
            
            chat_stats_text = f"<b>💬 Chat Statistics:</b><br/>"
            chat_stats_text += f"Average messages per chat: <b>{avg_messages}</b> • "
            chat_stats_text += f"Most active chat: <b>{most_active}</b> messages • "
            chat_stats_text += f"Longest chat: <b>{longest_chat}</b> messages"
            story.append(Paragraph(chat_stats_text, ParagraphStyle(
                'StatsStyle',
                parent=styles['Normal'],
                fontSize=10,
                textColor=HexColor('#475569'),
                leading=14,
                spaceAfter=4
            )))
    else:
        story.append(Paragraph("No chat sessions yet.", styles['Normal']))
    story.append(Spacer(1, 0.3*inch))
    
    # Preferences Section
    prefs = user.get("preferences", {})
    if prefs:
        story.append(Paragraph("Preferences", heading_style))
        prefs_data = [
            ['Setting', 'Value'],
            ['Theme', prefs.get("theme", "dark").capitalize()],
            ['Language', prefs.get("language", "en").upper()],
            ['Timezone', prefs.get("timezone", "UTC")],
            ['Date Format', prefs.get("date_format", "MM/DD/YYYY")],
            ['Email Notifications', "Enabled" if prefs.get("notifications", {}).get("email") else "Disabled"],
            ['Push Notifications', "Enabled" if prefs.get("notifications", {}).get("push") else "Disabled"],
            ['Product Updates', "Enabled" if prefs.get("notifications", {}).get("updates") else "Disabled"],
        ]
        prefs_table = Table(prefs_data, colWidths=[2.5*inch, 5*inch])
        prefs_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#1e40af')),
            ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('TOPPADDING', (0, 1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 8),
            ('LEFTPADDING', (0, 0), (-1, -1), 10),
            ('RIGHTPADDING', (0, 0), (-1, -1), 10),
            ('BACKGROUND', (0, 1), (-1, -1), HexColor('#f8fafc')),
            ('GRID', (0, 0), (-1, -1), 1, HexColor('#e2e8f0')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [HexColor('#ffffff'), HexColor('#f8fafc')]),
        ]))
        story.append(prefs_table)
        story.append(Spacer(1, 0.3*inch))
    
    # Footer
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph("This document contains your personal data from FastCite.", 
                          ParagraphStyle('Footer', parent=styles['Normal'], fontSize=9, 
                                       textColor=HexColor('#64748b'), alignment=TA_CENTER)))
    story.append(Paragraph(f"Generated on {datetime.utcnow().strftime('%B %d, %Y at %I:%M %p UTC')}", 
                          ParagraphStyle('Footer', parent=styles['Normal'], fontSize=9, 
                                       textColor=HexColor('#64748b'), alignment=TA_CENTER)))
    
    # Build PDF
    doc.build(story)
    buffer.seek(0)
    
    # Return PDF as streaming response
    filename = f"fastcite_user_data_{user.get('username')}_{datetime.utcnow().strftime('%Y%m%d')}.pdf"
    return StreamingResponse(
        io.BytesIO(buffer.read()),
        media_type="application/pdf",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"'
        }
    )

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

    # Delete user
    await users_collection.delete_one({"id": user_id})
    return {"message": "User account deleted successfully"}

# ----------------------------
# UPDATE USER (admin or self) - MUST BE AFTER all specific routes
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

    # 4. Check username uniqueness if username is being updated
    if "username" in updates:
        new_username = updates["username"]
        # Check if username is already taken by another user
        existing_user = await users_collection.find_one({"username": new_username})
        if existing_user and str(existing_user.get("id")) != str(user_id):
            raise HTTPException(
                status_code=400, 
                detail="Username already taken"
            )

    # 5. Apply updates
    await users_collection.update_one({"id": user_id}, {"$set": updates})

    # 6. Fetch updated user
    updated_user = await users_collection.find_one({"id": user_id})

    # 7. Create new token (always generate on update)
    token_data = {
        "sub": updated_user["username"],
        "role": updated_user["role"],
        "id": updated_user["id"],
    }
    new_access_token = create_access_token(token_data)

    # 8. Return response
    return {
        "message": "User updated successfully",
        "access_token": new_access_token,
    }

# ----------------------------
# GET USER BY ID (admin or self) - MUST BE LAST due to path parameter
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
