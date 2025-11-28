# database/auth.py
from fastapi import APIRouter, HTTPException, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, EmailStr
from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
from typing import Optional
from uuid import uuid4
import os
import random
from fastapi.responses import RedirectResponse, JSONResponse
from authlib.integrations.starlette_client import OAuth
from starlette.requests import Request
from dotenv import load_dotenv
from database.mongo import users_collection, db
from services.email_service import email_service
# Load .env (if present)
load_dotenv()

# ==============================
# CONFIGURATION
# ==============================
router = APIRouter(prefix="/auth", tags=["Authentication"])

# JWT Config (permanent login — no expiry)
SECRET_KEY = os.getenv("SECRET_KEY", "supersecretkey")
ALGORITHM = "HS256"

# Password Hashing (Argon2)
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

# OAuth2 Scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

# Ensure backend/frontend urls exist (use defaults if not provided)
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

# Validate critical Google env vars early (fail fast)
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")
if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
    # If you want the app to run *without* Google enabled, remove/adjust this raise.
    raise RuntimeError(
        "Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET environment variables. "
        "Add them to your .env or environment."
    )

# ==============================
# Pydantic Models
# ==============================
class UserCreate(BaseModel):
    username: str
    email: EmailStr
    password: str
    name: str
    dob: Optional[str] = None
    role: Optional[str] = "user"

class Token(BaseModel):
    access_token: str
    token_type: str

class Check2FARequest(BaseModel):
    username: str

class LoginWithPasswordRequest(BaseModel):
    username: str
    password: str

class LoginWith2FARequest(BaseModel):
    username: str
    two_factor_code: str

class ForgotPasswordRequest(BaseModel):
    username: str

class VerifyResetCodeRequest(BaseModel):
    username: str
    reset_code: str

class ResetPasswordRequest(BaseModel):
    username: str
    reset_code: str
    new_password: str

class RequestDeleteAccountCodeRequest(BaseModel):
    username: str

class VerifyDeleteAccountCodeRequest(BaseModel):
    username: str
    deletion_code: str

class SignupRequest(BaseModel):
    username: str
    email: EmailStr
    password: str
    name: str
    dob: Optional[str] = None
    role: Optional[str] = "user"

class VerifySignupCodeRequest(BaseModel):
    email: EmailStr
    verification_code: str

class CompleteSignupRequest(BaseModel):
    email: EmailStr
    verification_code: str
    username: str
    password: str
    name: str
    dob: Optional[str] = None
    role: Optional[str] = "user"

# ==============================
# Helper Functions
# ==============================
def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def create_access_token(data: dict):
    """Permanent token (no expiration)"""
    # add issued-at time for better introspection
    payload = data.copy()
    payload["iat"] = int(datetime.utcnow().timestamp())
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

# ==============================
# AUTH ROUTES
# ==============================
# Collection for pending signups (temporary storage until verification)
pending_signups_collection = db["pending_signups"]

@router.post("/signup", status_code=200)
async def signup(request: SignupRequest):
    """
    Step 1: Request signup - sends verification code to email.
    User data is stored temporarily until email is verified.
    """
    # Check if user already exists
    existing_user = await users_collection.find_one(
        {"$or": [{"username": request.username}, {"email": request.email}]}
    )
    if existing_user:
        raise HTTPException(status_code=400, detail="Username or email already registered")

    # Check if there's already a pending signup for this email
    existing_pending = await pending_signups_collection.find_one({"email": request.email})
    
    # Generate 6-digit verification code
    verification_code = str(random.randint(100000, 999999))
    
    # Store signup data temporarily with verification code
    pending_signup_data = {
        "email": request.email,
        "username": request.username,
        "password": request.password,  # Will be hashed on completion
        "name": request.name,
        "dob": request.dob,
        "role": request.role or "user",
        "verification_code": verification_code,
        "verification_code_expires": datetime.utcnow() + timedelta(minutes=10),
        "verification_code_used": False,
        "created_at": datetime.utcnow(),
    }
    
    # Update or insert pending signup
    if existing_pending:
        await pending_signups_collection.update_one(
            {"email": request.email},
            {"$set": pending_signup_data}
        )
    else:
        await pending_signups_collection.insert_one(pending_signup_data)
    
    # Send verification code email
    try:
        email_service.send_signup_verification_code_email(
            user_email=request.email,
            user_name=request.name,
            verification_code=verification_code
        )
    except Exception as e:
        print(f"⚠️ Failed to send verification email: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to send verification code. Please try again later."
        )
    
    return {"message": "Verification code sent to your email. Please check your inbox."}

@router.post("/verify-signup-code")
async def verify_signup_code(request: VerifySignupCodeRequest):
    """
    Step 2: Verify the signup code.
    """
    pending_signup = await pending_signups_collection.find_one({"email": request.email})
    
    if not pending_signup:
        raise HTTPException(status_code=404, detail="No pending signup found for this email")
    
    stored_code = pending_signup.get("verification_code")
    code_expires = pending_signup.get("verification_code_expires")
    code_used = pending_signup.get("verification_code_used", False)
    
    if not stored_code:
        raise HTTPException(status_code=400, detail="No verification code found. Please request a new one.")
    
    if code_used:
        raise HTTPException(status_code=400, detail="Verification code has already been used. Please request a new one.")
    
    if code_expires and datetime.utcnow() > code_expires:
        raise HTTPException(status_code=400, detail="Verification code has expired. Please request a new one.")
    
    if stored_code != request.verification_code:
        raise HTTPException(status_code=401, detail="Invalid verification code")
    
    return {"message": "Verification code verified successfully", "verified": True}

@router.post("/complete-signup", status_code=201)
async def complete_signup(request: CompleteSignupRequest):
    """
    Step 3: Complete signup after verification.
    Creates the actual user account.
    """
    # Verify the code first
    pending_signup = await pending_signups_collection.find_one({"email": request.email})
    
    if not pending_signup:
        raise HTTPException(status_code=404, detail="No pending signup found for this email")
    
    stored_code = pending_signup.get("verification_code")
    code_expires = pending_signup.get("verification_code_expires")
    code_used = pending_signup.get("verification_code_used", False)
    
    if not stored_code:
        raise HTTPException(status_code=400, detail="No verification code found. Please request a new one.")
    
    if code_used:
        raise HTTPException(status_code=400, detail="Verification code has already been used. Please request a new one.")
    
    if code_expires and datetime.utcnow() > code_expires:
        raise HTTPException(status_code=400, detail="Verification code has expired. Please request a new one.")
    
    if stored_code != request.verification_code:
        raise HTTPException(status_code=401, detail="Invalid verification code")
    
    # Verify that the data matches what was submitted
    if (pending_signup.get("username") != request.username or
        pending_signup.get("name") != request.name):
        raise HTTPException(status_code=400, detail="Signup data mismatch. Please start over.")
    
    # Check if user was created in the meantime
    existing_user = await users_collection.find_one(
        {"$or": [{"username": request.username}, {"email": request.email}]}
    )
    if existing_user:
        # Clean up pending signup
        await pending_signups_collection.delete_one({"email": request.email})
        raise HTTPException(status_code=400, detail="User already exists")
    
    # Create the user account
    user_dict = {
        "id": str(uuid4()),
        "username": request.username,
        "pass_hash": hash_password(request.password),
        "name": request.name,
        "dob": request.dob,
        "email": request.email,
        "role": request.role or "user",
        "created_at": datetime.utcnow(),
    }
    
    await users_collection.insert_one(user_dict)
    
    # Mark verification code as used and clean up pending signup
    await pending_signups_collection.delete_one({"email": request.email})
    
    # Send welcome email
    try:
        email_service.send_account_created_email(
            user_email=request.email,
            user_name=request.name,
            username=request.username
        )
    except Exception as e:
        print(f"⚠️ Failed to send welcome email: {e}")
    
    return {"message": "Account created successfully! You can now log in."}

@router.post("/check-2fa")
async def check_2fa_status(request: Check2FARequest):
    """
    Check if 2FA is enabled for a username.
    This endpoint is public and only checks if 2FA is enabled.
    """
    try:
        user = await users_collection.find_one({"username": request.username})
        if not user:
            # Don't reveal if username exists or not for security
            return {"username_exists": False, "two_factor_enabled": False}
        
        is_2fa_enabled = bool(user.get("two_factor_enabled") and user.get("two_factor_secret"))
        return {
            "username_exists": True,
            "two_factor_enabled": is_2fa_enabled
        }
    except Exception as e:
        # Handle MongoDB connection errors
        error_msg = str(e)
        if "ServerSelectionTimeoutError" in error_msg or "SSL handshake failed" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection failed. Please try again in a moment."
            )
        # Re-raise other exceptions
        raise

@router.post("/login", response_model=Token)
async def login(
    login_data: LoginWithPasswordRequest,
    request: Request = None
):
    """
    Login endpoint for accounts without 2FA.
    """
    try:
        user = await users_collection.find_one({"username": login_data.username})
    except Exception as e:
        # Handle MongoDB connection errors
        error_msg = str(e)
        if "ServerSelectionTimeoutError" in error_msg or "SSL handshake failed" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection failed. Please try again in a moment."
            )
        raise
    
    if not user or not verify_password(login_data.password, user["pass_hash"]):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    # Check if 2FA is enabled - if so, user should use login-2fa endpoint
    if user.get("two_factor_enabled") and user.get("two_factor_secret"):
        raise HTTPException(
            status_code=403,
            detail="2FA_REQUIRED: Two-factor authentication is enabled. Please use /auth/login-2fa endpoint."
        )

    # Update last login
    client_ip = request.client.host if request and request.client else None
    
    await users_collection.update_one(
        {"id": user["id"]},
        {"$set": {"last_login": datetime.utcnow()}}
    )
    
    # Include username + role in token
    token_data = {
        "sub": user["username"],
        "role": user["role"],
        "id": user["id"],
    }
    access_token = create_access_token(token_data)
    
    # Send login notification email
    try:
        email_service.send_login_success_email(
            user_email=user.get("email"),
            user_name=user.get("name", user.get("username")),
            login_time=datetime.utcnow(),
            ip_address=client_ip
        )
    except Exception as e:
        print(f"⚠️ Failed to send login email: {e}")
    
    return {"access_token": access_token, "token_type": "bearer"}

@router.post("/login-2fa", response_model=Token)
async def login_with_2fa(
    login_data: LoginWith2FARequest,
    request: Request = None
):
    """
    Login endpoint for accounts with 2FA enabled.
    This endpoint only requires username and 2FA code (password was verified in previous step).
    """
    try:
        user = await users_collection.find_one({"username": login_data.username})
    except Exception as e:
        # Handle MongoDB connection errors
        error_msg = str(e)
        if "ServerSelectionTimeoutError" in error_msg or "SSL handshake failed" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection failed. Please try again in a moment."
            )
        raise
    
    if not user:
        raise HTTPException(status_code=401, detail="Invalid username")

    # Verify 2FA is enabled
    if not user.get("two_factor_enabled") or not user.get("two_factor_secret"):
        raise HTTPException(
            status_code=400,
            detail="2FA is not enabled for this account"
        )
    
    if not login_data.two_factor_code:
        raise HTTPException(
            status_code=403,
            detail="Two-factor authentication code is required"
        )
    
    # Verify 2FA code
    import pyotp
    totp = pyotp.TOTP(user["two_factor_secret"])
    if not totp.verify(login_data.two_factor_code, valid_window=1):
        raise HTTPException(status_code=401, detail="Invalid 2FA code")

    # Include username + role in token
    token_data = {
        "sub": user["username"],
        "role": user["role"],
        "id": user["id"],
    }
    access_token = create_access_token(token_data)
    
    # Send login notification email
    try:
        from datetime import datetime
        client_ip = request.client.host if request and request.client else None
        email_service.send_login_success_email(
            user_email=user.get("email"),
            user_name=user.get("name", user.get("username")),
            login_time=datetime.utcnow(),
            ip_address=client_ip
        )
    except Exception as e:
        print(f"⚠️ Failed to send login email: {e}")
    
    return {"access_token": access_token, "token_type": "bearer"}

# ==============================
# FORGOT PASSWORD ROUTES
# ==============================
@router.post("/forgot-password")
async def forgot_password(request: ForgotPasswordRequest):
    """
    Request a password reset code. Sends a 6-digit code to the user's email.
    """
    try:
        user = await users_collection.find_one({"username": request.username})
    except Exception as e:
        error_msg = str(e)
        if "ServerSelectionTimeoutError" in error_msg or "SSL handshake failed" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection failed. Please try again in a moment."
            )
        raise
    
    # Don't reveal if username exists for security
    if not user:
        # Return success even if user doesn't exist (security best practice)
        return {"message": "If the username exists, a reset code has been sent to the registered email."}
    
    # Check if user has an email (required for password reset)
    if not user.get("email"):
        raise HTTPException(
            status_code=400,
            detail="No email address associated with this account. Please contact support."
        )
    
    # Generate 6-digit reset code
    reset_code = str(random.randint(100000, 999999))
    
    # Store reset code with expiration (10 minutes)
    reset_data = {
        "reset_code": reset_code,
        "reset_code_expires": datetime.utcnow() + timedelta(minutes=10),
        "reset_code_used": False
    }
    
    await users_collection.update_one(
        {"id": user["id"]},
        {"$set": reset_data}
    )
    
    # Send reset code email
    try:
        email_service.send_password_reset_code_email(
            user_email=user.get("email"),
            user_name=user.get("name", user.get("username", "User")),
            reset_code=reset_code
        )
    except Exception as e:
        print(f"⚠️ Failed to send password reset email: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to send reset code. Please try again later."
        )
    
    return {"message": "If the username exists, a reset code has been sent to the registered email."}

@router.post("/verify-reset-code")
async def verify_reset_code(request: VerifyResetCodeRequest):
    """
    Verify the password reset code.
    """
    try:
        user = await users_collection.find_one({"username": request.username})
    except Exception as e:
        error_msg = str(e)
        if "ServerSelectionTimeoutError" in error_msg or "SSL handshake failed" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection failed. Please try again in a moment."
            )
        raise
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    stored_code = user.get("reset_code")
    code_expires = user.get("reset_code_expires")
    code_used = user.get("reset_code_used", False)
    
    if not stored_code:
        raise HTTPException(status_code=400, detail="No reset code found. Please request a new one.")
    
    if code_used:
        raise HTTPException(status_code=400, detail="Reset code has already been used. Please request a new one.")
    
    if code_expires and datetime.utcnow() > code_expires:
        raise HTTPException(status_code=400, detail="Reset code has expired. Please request a new one.")
    
    if stored_code != request.reset_code:
        raise HTTPException(status_code=401, detail="Invalid reset code")
    
    return {"message": "Reset code verified successfully", "verified": True}

@router.post("/reset-password")
async def reset_password(request: ResetPasswordRequest):
    """
    Reset password using verified reset code.
    """
    try:
        user = await users_collection.find_one({"username": request.username})
    except Exception as e:
        error_msg = str(e)
        if "ServerSelectionTimeoutError" in error_msg or "SSL handshake failed" in error_msg:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Database connection failed. Please try again in a moment."
            )
        raise
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    stored_code = user.get("reset_code")
    code_expires = user.get("reset_code_expires")
    code_used = user.get("reset_code_used", False)
    
    if not stored_code:
        raise HTTPException(status_code=400, detail="No reset code found. Please request a new one.")
    
    if code_used:
        raise HTTPException(status_code=400, detail="Reset code has already been used. Please request a new one.")
    
    if code_expires and datetime.utcnow() > code_expires:
        raise HTTPException(status_code=400, detail="Reset code has expired. Please request a new one.")
    
    if stored_code != request.reset_code:
        raise HTTPException(status_code=401, detail="Invalid reset code")
    
    # Validate new password
    if not request.new_password or len(request.new_password) < 8:
        raise HTTPException(
            status_code=400,
            detail="New password must be at least 8 characters long"
        )
    
    # Hash and update password
    new_password_hash = hash_password(request.new_password)
    await users_collection.update_one(
        {"id": user["id"]},
        {
            "$set": {
                "pass_hash": new_password_hash,
                "last_password_change": datetime.utcnow(),
                "reset_code_used": True
            },
            "$unset": {
                "reset_code": "",
                "reset_code_expires": ""
            }
        }
    )
    
    # Send password change notification email
    try:
        email_service.send_password_changed_email(
            user_email=user.get("email"),
            user_name=user.get("name", user.get("username", "User"))
        )
    except Exception as e:
        print(f"⚠️ Failed to send password change email: {e}")
    
    return {"message": "Password reset successfully"}

# ==============================
# CURRENT USER DEPENDENCY (must be defined before routes that use it)
# ==============================
async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        user_id: str = payload.get("id")
        if username is None or user_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = await users_collection.find_one({"id": user_id})
    if user is None:
        raise credentials_exception

    user["role"] = role
    return user

# ==============================
# ACCOUNT DELETION VERIFICATION ROUTES
# ==============================
@router.post("/request-delete-account-code")
async def request_delete_account_code(request: RequestDeleteAccountCodeRequest, current_user: dict = Depends(get_current_user)):
    """
    Request an account deletion verification code. Sends a 6-digit code to the user's email.
    Only the user themselves can request a deletion code.
    """
    # Verify user can only request code for their own account
    if str(current_user["username"]) != request.username:
        raise HTTPException(status_code=403, detail="You can only request deletion code for your own account")
    
    user = await users_collection.find_one({"username": request.username})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Check if user has an email (required for deletion verification)
    if not user.get("email"):
        raise HTTPException(
            status_code=400,
            detail="No email address associated with this account. Please contact support."
        )
    
    # Generate 6-digit deletion code
    deletion_code = str(random.randint(100000, 999999))
    
    # Store deletion code with expiration (10 minutes)
    deletion_data = {
        "deletion_code": deletion_code,
        "deletion_code_expires": datetime.utcnow() + timedelta(minutes=10),
        "deletion_code_used": False
    }
    
    await users_collection.update_one(
        {"id": user["id"]},
        {"$set": deletion_data}
    )
    
    # Send deletion code email
    try:
        email_service.send_account_deletion_code_email(
            user_email=user.get("email"),
            user_name=user.get("name", user.get("username", "User")),
            deletion_code=deletion_code
        )
    except Exception as e:
        print(f"⚠️ Failed to send account deletion code email: {e}")
        raise HTTPException(
            status_code=500,
            detail="Failed to send deletion code. Please try again later."
        )
    
    return {"message": "Deletion verification code sent to your email. Please check your inbox."}

@router.post("/verify-delete-account-code")
async def verify_delete_account_code(request: VerifyDeleteAccountCodeRequest, current_user: dict = Depends(get_current_user)):
    """
    Verify the account deletion code.
    Only the user themselves can verify their deletion code.
    """
    # Verify user can only verify code for their own account
    if str(current_user["username"]) != request.username:
        raise HTTPException(status_code=403, detail="You can only verify deletion code for your own account")
    
    user = await users_collection.find_one({"username": request.username})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    stored_code = user.get("deletion_code")
    code_expires = user.get("deletion_code_expires")
    code_used = user.get("deletion_code_used", False)
    
    if not stored_code:
        raise HTTPException(status_code=400, detail="No deletion code found. Please request a new one.")
    
    if code_used:
        raise HTTPException(status_code=400, detail="Deletion code has already been used. Please request a new one.")
    
    if code_expires and datetime.utcnow() > code_expires:
        raise HTTPException(status_code=400, detail="Deletion code has expired. Please request a new one.")
    
    if stored_code != request.deletion_code:
        raise HTTPException(status_code=401, detail="Invalid deletion code")
    
    return {"message": "Deletion code verified successfully", "verified": True}

# ==============================
# CURRENT USER DEPENDENCY (must be defined before routes that use it)
# ==============================
async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    user = await users_collection.find_one({"username": username})
    if user is None:
        raise credentials_exception

    user["role"] = role
    return user

# ==============================
# ADMIN DEPENDENCY
# ==============================
async def get_admin_user(current_user: dict = Depends(get_current_user)):
    """Dependency to ensure the current user is an admin."""
    if str(current_user.get("role")) != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required"
        )
    return current_user

# ==============================
# GOOGLE OAUTH SETUP
# ==============================
oauth = OAuth()
oauth.register(
    name="google",
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",
    client_kwargs={"scope": "openid email profile"},
)

# ==============================
# GOOGLE LOGIN ROUTES
# ==============================
@router.get("/google/login")
async def google_login(request: Request):
    """
    Step 1: Redirect user to Google login page
    """
    # BACKEND_URL is guaranteed to be a string (defaulted above)
    redirect_uri = f"{BACKEND_URL}/auth/google/callback"
    return await oauth.google.authorize_redirect(request, redirect_uri)

@router.get("/google/callback")
async def google_callback(request: Request):
    """
    Step 2: Handle Google's callback, create/find user, issue JWT
    """
    try:
        token = await oauth.google.authorize_access_token(request)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error obtaining access token: {e}")

    # Try common places for user info
    user_info = None
    # Some providers return userinfo in token['userinfo'], some require userinfo endpoint call
    if isinstance(token, dict):
        user_info = token.get("userinfo") or token.get("id_token_claims") or None

    # Fallback: call userinfo endpoint directly
    if not user_info:
        try:
            # authlib exposes `.userinfo()` convenience on the remote app in many versions
            # but to be broadly compatible, attempt `.userinfo()` then `.get('userinfo')` fallback.
            user_info = await oauth.google.userinfo(token=token)
        except Exception:
            # final fallback: try a raw GET to the userinfo endpoint
            try:
                resp = await oauth.google.get("userinfo", token=token)
                user_info = resp.json() if resp and resp.status_code == 200 else None
            except Exception:
                user_info = None

    if not user_info or "email" not in user_info:
        # Return an explicit failure that frontend can handle
        return JSONResponse(
            status_code=400,
            content={"detail": "Google login failed: could not obtain user info"},
        )

    email = user_info["email"]
    name = user_info.get("name", email.split("@")[0])
    google_id = user_info.get("sub") or user_info.get("id")  # different providers use different keys

    # Check if user already exists
    existing_user = await users_collection.find_one({"email": email})

    if not existing_user:
        # Create new user
        new_user = {
            "id": str(uuid4()),
            "username": email.split("@")[0],
            "email": email,
            "name": name,
            "role": "user",
            "google_id": google_id,
            "created_at": datetime.utcnow(),
        }
        await users_collection.insert_one(new_user)
        user = new_user
    else:
        # Optionally update google_id if missing
        if google_id and existing_user.get("google_id") != google_id:
            await users_collection.update_one(
                {"_id": existing_user["_id"]},
                {"$set": {"google_id": google_id}}
            )
        user = existing_user

    # Create JWT for our app
    token_data = {"sub": user["username"], "role": user["role"]}
    access_token = create_access_token(token_data)

    # Redirect to frontend with token
# Redirect to frontend callback route with token
    frontend_url = f"{FRONTEND_URL.rstrip('/')}/auth/google/callback?token={access_token}"
    return RedirectResponse(url=frontend_url)

