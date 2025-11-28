"""
Email Service for sending transactional emails.
Supports account creation, password changes, book operations, and login notifications.
Supports both basic authentication and OAuth2 (for Outlook/Office 365).
"""
import os
import smtplib
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Email Configuration
SMTP_SERVER = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USERNAME = os.getenv("SMTP_USERNAME")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")
EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USERNAME)
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "FastCite")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:5173")

# OAuth2 Configuration (for Outlook/Office 365)
SMTP_AUTH_TYPE = os.getenv("SMTP_AUTH_TYPE", "basic")  # "basic" or "oauth2"
OAUTH_CLIENT_ID = os.getenv("OAUTH_CLIENT_ID")
OAUTH_CLIENT_SECRET = os.getenv("OAUTH_CLIENT_SECRET")
OAUTH_TENANT_ID = os.getenv("OAUTH_TENANT_ID", "common")  # "common" for personal accounts
OAUTH_AUTHORITY = os.getenv("OAUTH_AUTHORITY", f"https://login.microsoftonline.com/{OAUTH_TENANT_ID}")


class EmailService:
    """Service for sending emails via SMTP with support for basic auth and OAuth2."""
    
    def __init__(self):
        self.smtp_server = SMTP_SERVER
        self.smtp_port = SMTP_PORT
        self.smtp_username = SMTP_USERNAME
        self.smtp_password = SMTP_PASSWORD
        self.email_from = EMAIL_FROM
        self.email_from_name = EMAIL_FROM_NAME
        self.auth_type = SMTP_AUTH_TYPE.lower()
        self.oauth_client_id = OAUTH_CLIENT_ID
        self.oauth_client_secret = OAUTH_CLIENT_SECRET
        self.oauth_authority = OAUTH_AUTHORITY
        self._oauth_token_cache = None
        self._oauth_token_expiry = None
    
    def _get_oauth2_token(self) -> Optional[str]:
        """
        Get OAuth2 access token for Microsoft/Outlook.
        Uses MSAL (Microsoft Authentication Library) for token acquisition.
        """
        if not self.oauth_client_id or not self.oauth_client_secret:
            print("⚠️ OAuth2 credentials not configured")
            return None
        
        # Check if we have a cached valid token
        if self._oauth_token_cache and self._oauth_token_expiry:
            if datetime.utcnow() < self._oauth_token_expiry:
                return self._oauth_token_cache
        
        try:
            from msal import ConfidentialClientApplication
            
            # Create MSAL app instance
            app = ConfidentialClientApplication(
                client_id=self.oauth_client_id,
                client_credential=self.oauth_client_secret,
                authority=self.oauth_authority
            )
            
            # Request token with SMTP.Send scope
            scopes = ["https://outlook.office365.com/.default"]
            result = app.acquire_token_for_client(scopes=scopes)
            
            if "access_token" in result:
                access_token = result["access_token"]
                # Cache token (tokens typically expire in 1 hour)
                expires_in = result.get("expires_in", 3600)
                self._oauth_token_cache = access_token
                self._oauth_token_expiry = datetime.utcnow().replace(
                    microsecond=0
                ) + timedelta(seconds=expires_in - 300)  # Refresh 5 min before expiry
                print("✅ OAuth2 token acquired successfully")
                return access_token
            else:
                error = result.get("error_description", result.get("error", "Unknown error"))
                print(f"❌ Failed to acquire OAuth2 token: {error}")
                return None
                
        except ImportError:
            print("❌ MSAL library not installed. Install with: pip install msal")
            return None
        except Exception as e:
            print(f"❌ Error acquiring OAuth2 token: {e}")
            return None
    
    def _authenticate_smtp_oauth2(self, server: smtplib.SMTP, email: str, access_token: str) -> bool:
        """
        Authenticate SMTP connection using OAuth2 (XOAUTH2 method).
        
        Args:
            server: SMTP server connection
            email: Email address to authenticate as
            access_token: OAuth2 access token
            
        Returns:
            True if authentication successful, False otherwise
        """
        try:
            # XOAUTH2 authentication string format
            auth_string = f"user={email}\x01auth=Bearer {access_token}\x01\x01"
            auth_bytes = base64.b64encode(auth_string.encode()).decode()
            
            # Send AUTH XOAUTH2 command
            code, response = server.docmd("AUTH", "XOAUTH2 " + auth_bytes)
            
            if code == 235:  # 235 = Authentication successful
                print("✅ OAuth2 authentication successful")
                return True
            else:
                print(f"❌ OAuth2 authentication failed: {code} {response}")
                return False
                
        except Exception as e:
            print(f"❌ OAuth2 authentication error: {e}")
            return False
        
    def _send_email(
        self,
        to_email: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None
    ) -> bool:
        """
        Send an email using SMTP with either basic auth or OAuth2.
        
        Args:
            to_email: Recipient email address
            subject: Email subject
            html_body: HTML email body
            text_body: Plain text email body (optional)
            
        Returns:
            True if email sent successfully, False otherwise
        """
        # Skip sending if SMTP is not configured
        if self.auth_type == "oauth2":
            if not self.oauth_client_id or not self.oauth_client_secret:
                print(f"⚠️ OAuth2 not configured. Would send to {to_email}: {subject}")
                return False
        else:
            if not self.smtp_username or not self.smtp_password:
                print(f"⚠️ Email not configured. Would send to {to_email}: {subject}")
                return False
            
        try:
            # Create message
            msg = MIMEMultipart("alternative")
            msg["From"] = f"{self.email_from_name} <{self.email_from}>"
            msg["To"] = to_email
            msg["Subject"] = subject
            
            # Add text and HTML parts
            if text_body:
                text_part = MIMEText(text_body, "plain")
                msg.attach(text_part)
            
            html_part = MIMEText(html_body, "html")
            msg.attach(html_part)
            
            # Send email with appropriate authentication
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                
                if self.auth_type == "oauth2":
                    # OAuth2 authentication
                    access_token = self._get_oauth2_token()
                    if not access_token:
                        print(f"❌ Failed to get OAuth2 token for {to_email}")
                        return False
                    
                    if not self._authenticate_smtp_oauth2(server, self.email_from, access_token):
                        print(f"❌ OAuth2 authentication failed for {to_email}")
                        return False
                else:
                    # Basic authentication
                    server.login(self.smtp_username, self.smtp_password)
                
                server.send_message(msg)
            
            print(f"✅ Email sent to {to_email}: {subject}")
            return True
            
        except Exception as e:
            print(f"❌ Failed to send email to {to_email}: {e}")
            return False
    
    def send_account_created_email(self, user_email: str, user_name: str, username: str):
        """Send welcome email when account is created."""
        subject = "Welcome to FastCite! 🎉"
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #2d3748; background-color: #f7fafc; padding: 20px; }}
                .email-wrapper {{ max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 50px 30px; text-align: center; position: relative; }}
                .header::after {{ content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 4px; background: linear-gradient(90deg, rgba(255,255,255,0.3) 0%, rgba(255,255,255,0.1) 100%); }}
                .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 10px; letter-spacing: -0.5px; }}
                .header p {{ font-size: 16px; opacity: 0.95; }}
                .content {{ padding: 40px 35px; background-color: #ffffff; }}
                .greeting {{ font-size: 18px; color: #1a202c; margin-bottom: 20px; font-weight: 600; }}
                .message {{ font-size: 16px; color: #4a5568; margin-bottom: 30px; line-height: 1.8; }}
                .account-details {{ background: linear-gradient(135deg, #f7fafc 0%, #edf2f7 100%); border-left: 4px solid #667eea; padding: 25px; border-radius: 8px; margin: 30px 0; }}
                .account-details h3 {{ font-size: 16px; color: #2d3748; margin-bottom: 15px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; font-size: 12px; }}
                .detail-item {{ display: flex; justify-content: space-between; padding: 12px 0; border-bottom: 1px solid #e2e8f0; }}
                .detail-item:last-child {{ border-bottom: none; }}
                .detail-label {{ color: #718096; font-size: 14px; }}
                .detail-value {{ color: #2d3748; font-size: 14px; font-weight: 600; }}
                .button-container {{ text-align: center; margin: 35px 0; }}
                .button {{ display: inline-block; padding: 16px 40px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; text-decoration: none; border-radius: 8px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4); transition: transform 0.2s; }}
                .button:hover {{ transform: translateY(-2px); box-shadow: 0 6px 16px rgba(102, 126, 234, 0.5); }}
                .footer-text {{ font-size: 14px; color: #718096; margin-top: 30px; line-height: 1.8; }}
                .footer {{ background-color: #f7fafc; padding: 25px 35px; text-align: center; border-top: 1px solid #e2e8f0; }}
                .footer p {{ font-size: 12px; color: #a0aec0; margin: 5px 0; }}
                .footer .brand {{ color: #667eea; font-weight: 600; }}
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                <div class="header">
                    <h1>🎉 Welcome to FastCite!</h1>
                    <p>Your journey to smarter research starts here</p>
                </div>
                <div class="content">
                    <div class="greeting">Hello {user_name},</div>
                    <div class="message">
                        Thank you for joining FastCite! We're thrilled to have you on board. You're now part of a community that's revolutionizing how researchers and students work with academic documents.
                    </div>
                    <div class="account-details">
                        <h3>Your Account Information</h3>
                        <div class="detail-item">
                            <span class="detail-label">Username</span>
                            <span class="detail-value">{username}</span>
                        </div>
                        <div class="detail-item">
                            <span class="detail-label">Email</span>
                            <span class="detail-value">{user_email}</span>
                        </div>
                    </div>
                    <div class="message">
                        You can now start uploading PDFs and leverage our AI-powered citation and research tools to enhance your academic workflow.
                    </div>
                    <div class="button-container">
                        <a href="{FRONTEND_URL}/dashboard" class="button">Get Started →</a>
                    </div>
                    <div class="footer-text">
                        If you have any questions or need assistance, our support team is here to help. Just reach out anytime!
                    </div>
                </div>
                <div class="footer">
                    <p>Best regards,<br><span class="brand">The FastCite Team</span></p>
                    <p style="margin-top: 15px;">This is an automated email. Please do not reply to this message.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        Welcome to FastCite!
        
        Hello {user_name},
        
        Thank you for creating an account with FastCite! We're excited to have you on board.
        
        Your account details:
        - Username: {username}
        - Email: {user_email}
        
        You can now start uploading PDFs and using our AI-powered citation and research tools.
        
        Visit {FRONTEND_URL}/dashboard to get started.
        
        Best regards,
        The FastCite Team
        """
        
        return self._send_email(user_email, subject, html_body, text_body)
    
    def send_password_changed_email(self, user_email: str, user_name: str):
        """Send email notification when password is changed."""
        subject = "Password Changed Successfully 🔒"
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #2d3748; background-color: #f7fafc; padding: 20px; }}
                .email-wrapper {{ max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }}
                .header {{ background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); color: white; padding: 50px 30px; text-align: center; position: relative; }}
                .header::after {{ content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 4px; background: linear-gradient(90deg, rgba(255,255,255,0.3) 0%, rgba(255,255,255,0.1) 100%); }}
                .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 10px; letter-spacing: -0.5px; }}
                .header .icon {{ font-size: 48px; margin-bottom: 10px; }}
                .content {{ padding: 40px 35px; background-color: #ffffff; }}
                .greeting {{ font-size: 18px; color: #1a202c; margin-bottom: 20px; font-weight: 600; }}
                .message {{ font-size: 16px; color: #4a5568; margin-bottom: 25px; line-height: 1.8; }}
                .security-alert {{ background: linear-gradient(135deg, #fff5e6 0%, #ffe6cc 100%); border-left: 4px solid #ff9800; padding: 20px; border-radius: 8px; margin: 30px 0; }}
                .security-alert .alert-icon {{ font-size: 24px; margin-bottom: 10px; }}
                .security-alert .alert-title {{ font-size: 16px; font-weight: 700; color: #e65100; margin-bottom: 8px; }}
                .security-alert .alert-text {{ font-size: 14px; color: #bf360c; line-height: 1.6; }}
                .info-box {{ background: #f7fafc; border: 1px solid #e2e8f0; padding: 20px; border-radius: 8px; margin: 25px 0; }}
                .info-box .info-label {{ font-size: 12px; color: #718096; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 5px; }}
                .info-box .info-value {{ font-size: 16px; color: #2d3748; font-weight: 600; }}
                .footer {{ background-color: #f7fafc; padding: 25px 35px; text-align: center; border-top: 1px solid #e2e8f0; }}
                .footer p {{ font-size: 12px; color: #a0aec0; margin: 5px 0; }}
                .footer .brand {{ color: #f5576c; font-weight: 600; }}
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                <div class="header">
                    <div class="icon">🔒</div>
                    <h1>Password Changed</h1>
                    <p>Your account security has been updated</p>
                </div>
                <div class="content">
                    <div class="greeting">Hello {user_name},</div>
                    <div class="message">
                        Your password has been successfully changed. Your account is now secured with your new password.
                    </div>
                    <div class="info-box">
                        <div class="info-label">Change Time</div>
                        <div class="info-value">{datetime.utcnow().strftime("%B %d, %Y at %I:%M %p UTC")}</div>
                    </div>
                    <div class="security-alert">
                        <div class="alert-icon">⚠️</div>
                        <div class="alert-title">Security Notice</div>
                        <div class="alert-text">
                            If you did not make this change, please contact our support team immediately. Your account security may be compromised.
                        </div>
                    </div>
                    <div class="message">
                        If you made this change, you can safely ignore this email. This is just a security notification to keep you informed.
                    </div>
                </div>
                <div class="footer">
                    <p>Best regards,<br><span class="brand">The FastCite Team</span></p>
                    <p style="margin-top: 15px;">This is an automated security notification. Please do not reply to this message.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        Password Changed Successfully
        
        Hello {user_name},
        
        Your password has been successfully changed.
        
        Time: {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")}
        
        ⚠️ Security Notice: If you did not make this change, please contact our support team immediately.
        
        If you made this change, you can safely ignore this email.
        
        Best regards,
        The FastCite Team
        """
        
        return self._send_email(user_email, subject, html_body, text_body)
    
    def send_book_uploaded_email(self, user_email: str, user_name: str, book_name: str, book_id: str):
        """Send email notification when a book is uploaded."""
        subject = f"Book '{book_name}' Uploaded Successfully 📚"
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #2d3748; background-color: #f7fafc; padding: 20px; }}
                .email-wrapper {{ max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }}
                .header {{ background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); color: white; padding: 50px 30px; text-align: center; position: relative; }}
                .header::after {{ content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 4px; background: linear-gradient(90deg, rgba(255,255,255,0.3) 0%, rgba(255,255,255,0.1) 100%); }}
                .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 10px; letter-spacing: -0.5px; }}
                .header .icon {{ font-size: 48px; margin-bottom: 10px; }}
                .content {{ padding: 40px 35px; background-color: #ffffff; }}
                .greeting {{ font-size: 18px; color: #1a202c; margin-bottom: 20px; font-weight: 600; }}
                .message {{ font-size: 16px; color: #4a5568; margin-bottom: 25px; line-height: 1.8; }}
                .book-card {{ background: linear-gradient(135deg, #e6f7ff 0%, #bae7ff 100%); border: 2px solid #4facfe; padding: 30px; border-radius: 12px; margin: 30px 0; box-shadow: 0 2px 8px rgba(79, 172, 254, 0.15); }}
                .book-card .book-title {{ font-size: 24px; font-weight: 700; color: #0050b3; margin-bottom: 15px; }}
                .status-badge {{ display: inline-block; background: #4facfe; color: white; padding: 8px 16px; border-radius: 20px; font-size: 14px; font-weight: 600; margin-top: 10px; }}
                .status-badge::before {{ content: '⏳ '; }}
                .book-card .book-description {{ font-size: 14px; color: #0050b3; margin-top: 15px; line-height: 1.6; }}
                .button-container {{ text-align: center; margin: 35px 0; }}
                .button {{ display: inline-block; padding: 16px 40px; background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); color: white; text-decoration: none; border-radius: 8px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(79, 172, 254, 0.4); transition: transform 0.2s; }}
                .button:hover {{ transform: translateY(-2px); box-shadow: 0 6px 16px rgba(79, 172, 254, 0.5); }}
                .footer {{ background-color: #f7fafc; padding: 25px 35px; text-align: center; border-top: 1px solid #e2e8f0; }}
                .footer p {{ font-size: 12px; color: #a0aec0; margin: 5px 0; }}
                .footer .brand {{ color: #4facfe; font-weight: 600; }}
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                <div class="header">
                    <div class="icon">📚</div>
                    <h1>Book Uploaded!</h1>
                    <p>Your document is being processed</p>
                </div>
                <div class="content">
                    <div class="greeting">Hello {user_name},</div>
                    <div class="message">
                        Great news! Your book has been successfully uploaded to FastCite and is now being processed by our AI system.
                    </div>
                    <div class="book-card">
                        <div class="book-title">{book_name}</div>
                        <div class="status-badge">Processing</div>
                        <div class="book-description">
                            Our AI is analyzing your document, extracting key information, and preparing it for intelligent search and citation. This usually takes just a few minutes.
                        </div>
                    </div>
                    <div class="message">
                        You'll receive another notification once processing is complete. In the meantime, you can check the status in your dashboard.
                    </div>
                    <div class="button-container">
                        <a href="{FRONTEND_URL}/manage" class="button">View My Books →</a>
                    </div>
                </div>
                <div class="footer">
                    <p>Best regards,<br><span class="brand">The FastCite Team</span></p>
                    <p style="margin-top: 15px;">This is an automated email. Please do not reply to this message.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        Book Uploaded Successfully
        
        Hello {user_name},
        
        Your book "{book_name}" has been successfully uploaded and is now being processed.
        
        Status: Processing
        
        We're currently processing your book. You'll be notified once it's ready to use.
        
        Visit {FRONTEND_URL}/manage to view your books and check the processing status.
        
        Best regards,
        The FastCite Team
        """
        
        return self._send_email(user_email, subject, html_body, text_body)
    
    def send_book_deleted_email(self, user_email: str, user_name: str, book_name: str):
        """Send email notification when a book is deleted."""
        subject = f"Book '{book_name}' Deleted 📚"
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #2d3748; background-color: #f7fafc; padding: 20px; }}
                .email-wrapper {{ max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }}
                .header {{ background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); color: white; padding: 50px 30px; text-align: center; position: relative; }}
                .header::after {{ content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 4px; background: linear-gradient(90deg, rgba(255,255,255,0.3) 0%, rgba(255,255,255,0.1) 100%); }}
                .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 10px; letter-spacing: -0.5px; }}
                .header .icon {{ font-size: 48px; margin-bottom: 10px; }}
                .content {{ padding: 40px 35px; background-color: #ffffff; }}
                .greeting {{ font-size: 18px; color: #1a202c; margin-bottom: 20px; font-weight: 600; }}
                .message {{ font-size: 16px; color: #4a5568; margin-bottom: 25px; line-height: 1.8; }}
                .book-card {{ background: linear-gradient(135deg, #fff5f5 0%, #ffe0e0 100%); border: 2px solid #fa709a; padding: 30px; border-radius: 12px; margin: 30px 0; box-shadow: 0 2px 8px rgba(250, 112, 154, 0.15); }}
                .book-card .book-title {{ font-size: 24px; font-weight: 700; color: #c53030; margin-bottom: 15px; }}
                .status-badge {{ display: inline-block; background: #fc8181; color: white; padding: 8px 16px; border-radius: 20px; font-size: 14px; font-weight: 600; margin-top: 10px; }}
                .status-badge::before {{ content: '🗑️ '; }}
                .info-box {{ background: #f7fafc; border: 1px solid #e2e8f0; padding: 20px; border-radius: 8px; margin: 25px 0; }}
                .info-box .info-label {{ font-size: 12px; color: #718096; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 5px; }}
                .info-box .info-value {{ font-size: 16px; color: #2d3748; font-weight: 600; }}
                .security-alert {{ background: linear-gradient(135deg, #fff5e6 0%, #ffe6cc 100%); border-left: 4px solid #ff9800; padding: 20px; border-radius: 8px; margin: 30px 0; }}
                .security-alert .alert-icon {{ font-size: 24px; margin-bottom: 10px; }}
                .security-alert .alert-title {{ font-size: 16px; font-weight: 700; color: #e65100; margin-bottom: 8px; }}
                .security-alert .alert-text {{ font-size: 14px; color: #bf360c; line-height: 1.6; }}
                .button-container {{ text-align: center; margin: 35px 0; }}
                .button {{ display: inline-block; padding: 16px 40px; background: linear-gradient(135deg, #fa709a 0%, #fee140 100%); color: white; text-decoration: none; border-radius: 8px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(250, 112, 154, 0.4); transition: transform 0.2s; }}
                .button:hover {{ transform: translateY(-2px); box-shadow: 0 6px 16px rgba(250, 112, 154, 0.5); }}
                .footer {{ background-color: #f7fafc; padding: 25px 35px; text-align: center; border-top: 1px solid #e2e8f0; }}
                .footer p {{ font-size: 12px; color: #a0aec0; margin: 5px 0; }}
                .footer .brand {{ color: #fa709a; font-weight: 600; }}
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                <div class="header">
                    <div class="icon">📚</div>
                    <h1>Book Deleted</h1>
                    <p>Removal confirmed</p>
                </div>
                <div class="content">
                    <div class="greeting">Hello {user_name},</div>
                    <div class="message">
                        Your book has been successfully deleted from your FastCite account. All associated data has been permanently removed.
                    </div>
                    <div class="book-card">
                        <div class="book-title">{book_name}</div>
                        <div class="status-badge">Deleted</div>
                    </div>
                    <div class="info-box">
                        <div class="info-label">Deletion Time</div>
                        <div class="info-value">{datetime.utcnow().strftime("%B %d, %Y at %I:%M %p UTC")}</div>
                    </div>
                    <div class="message">
                        All associated data, including document chunks, embeddings, and metadata, have been permanently removed from our systems.
                    </div>
                    <div class="security-alert">
                        <div class="alert-icon">⚠️</div>
                        <div class="alert-title">Important Notice</div>
                        <div class="alert-text">
                            If you did not delete this book, please contact our support team immediately. Your account may have been compromised.
                        </div>
                    </div>
                    <div class="button-container">
                        <a href="{FRONTEND_URL}/manage" class="button">View My Books →</a>
                    </div>
                </div>
                <div class="footer">
                    <p>Best regards,<br><span class="brand">The FastCite Team</span></p>
                    <p style="margin-top: 15px;">This is an automated email. Please do not reply to this message.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        Book Deleted
        
        Hello {user_name},
        
        Your book "{book_name}" has been successfully deleted from your account.
        
        Status: Deleted
        Time: {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")}
        
        All associated data, including chunks and embeddings, have been removed.
        
        If you did not delete this book, please contact our support team immediately.
        
        Best regards,
        The FastCite Team
        """
        
        return self._send_email(user_email, subject, html_body, text_body)
    
    def send_book_deleted_by_admin_email(self, user_email: str, user_name: str, book_name: str):
        """Send email notification when a book is deleted by an admin."""
        subject = f"Book '{book_name}' Removed by Administrator 📚"
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #2d3748; background-color: #f7fafc; padding: 20px; }}
                .email-wrapper {{ max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }}
                .header {{ background: linear-gradient(135deg, #dc2626 0%, #ef4444 100%); color: white; padding: 50px 30px; text-align: center; position: relative; }}
                .header::after {{ content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 4px; background: linear-gradient(90deg, rgba(255,255,255,0.3) 0%, rgba(255,255,255,0.1) 100%); }}
                .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 10px; letter-spacing: -0.5px; }}
                .header .icon {{ font-size: 48px; margin-bottom: 10px; }}
                .content {{ padding: 40px 35px; background-color: #ffffff; }}
                .greeting {{ font-size: 18px; color: #1a202c; margin-bottom: 20px; font-weight: 600; }}
                .message {{ font-size: 16px; color: #4a5568; margin-bottom: 25px; line-height: 1.8; }}
                .book-card {{ background: linear-gradient(135deg, #fff5f5 0%, #ffe0e0 100%); border: 2px solid #dc2626; padding: 30px; border-radius: 12px; margin: 30px 0; box-shadow: 0 2px 8px rgba(220, 38, 38, 0.15); }}
                .book-card .book-title {{ font-size: 24px; font-weight: 700; color: #c53030; margin-bottom: 15px; }}
                .status-badge {{ display: inline-block; background: #dc2626; color: white; padding: 8px 16px; border-radius: 20px; font-size: 14px; font-weight: 600; margin-top: 10px; }}
                .status-badge::before {{ content: '🔒 '; }}
                .info-box {{ background: #f7fafc; border: 1px solid #e2e8f0; padding: 20px; border-radius: 8px; margin: 25px 0; }}
                .info-box .info-label {{ font-size: 12px; color: #718096; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 5px; }}
                .info-box .info-value {{ font-size: 16px; color: #2d3748; font-weight: 600; }}
                .admin-alert {{ background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%); border-left: 4px solid #dc2626; padding: 20px; border-radius: 8px; margin: 30px 0; }}
                .admin-alert .alert-icon {{ font-size: 24px; margin-bottom: 10px; }}
                .admin-alert .alert-title {{ font-size: 16px; font-weight: 700; color: #991b1b; margin-bottom: 8px; }}
                .admin-alert .alert-text {{ font-size: 14px; color: #7f1d1d; line-height: 1.6; }}
                .button-container {{ text-align: center; margin: 35px 0; }}
                .button {{ display: inline-block; padding: 16px 40px; background: linear-gradient(135deg, #dc2626 0%, #ef4444 100%); color: white; text-decoration: none; border-radius: 8px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(220, 38, 38, 0.4); transition: transform 0.2s; }}
                .button:hover {{ transform: translateY(-2px); box-shadow: 0 6px 16px rgba(220, 38, 38, 0.5); }}
                .footer {{ background-color: #f7fafc; padding: 25px 35px; text-align: center; border-top: 1px solid #e2e8f0; }}
                .footer p {{ font-size: 12px; color: #a0aec0; margin: 5px 0; }}
                .footer .brand {{ color: #dc2626; font-weight: 600; }}
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                <div class="header">
                    <div class="icon">🔒</div>
                    <h1>Book Removed by Administrator</h1>
                    <p>Administrative action taken</p>
                </div>
                <div class="content">
                    <div class="greeting">Hello {user_name},</div>
                    <div class="message">
                        We are writing to inform you that the book listed below has been removed from your FastCite account by a system administrator.
                    </div>
                    <div class="book-card">
                        <div class="book-title">{book_name}</div>
                        <div class="status-badge">Removed by Admin</div>
                    </div>
                    <div class="info-box">
                        <div class="info-label">Removal Time</div>
                        <div class="info-value">{datetime.utcnow().strftime("%B %d, %Y at %I:%M %p UTC")}</div>
                    </div>
                    <div class="message">
                        All associated data, including document chunks, embeddings, and metadata, have been permanently removed from our systems.
                    </div>
                    <div class="admin-alert">
                        <div class="alert-icon">⚠️</div>
                        <div class="alert-title">Administrative Removal</div>
                        <div class="alert-text">
                            This book was removed by a FastCite administrator. This action may have been taken due to policy violations, content issues, or other administrative reasons. If you have questions about this removal, please contact our support team.
                        </div>
                    </div>
                    <div class="button-container">
                        <a href="{FRONTEND_URL}/manage" class="button">View My Books →</a>
                    </div>
                </div>
                <div class="footer">
                    <p>Best regards,<br><span class="brand">The FastCite Team</span></p>
                    <p style="margin-top: 15px;">This is an automated email. Please do not reply to this message.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        Book Removed by Administrator
        
        Hello {user_name},
        
        We are writing to inform you that the book "{book_name}" has been removed from your account by a FastCite administrator.
        
        Status: Removed by Admin
        Time: {datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")}
        
        All associated data, including chunks and embeddings, have been permanently removed.
        
        This book was removed by a system administrator. This action may have been taken due to policy violations, content issues, or other administrative reasons.
        
        If you have questions about this removal, please contact our support team.
        
        Best regards,
        The FastCite Team
        """
        
        return self._send_email(user_email, subject, html_body, text_body)
    
    def send_login_success_email(self, user_email: str, user_name: str, login_time: datetime, ip_address: Optional[str] = None):
        """Send email notification on successful login."""
        subject = "New Login Detected 🔐"
        
        ip_info = f"""
                        <div class="info-item">
                            <span class="info-label">IP Address</span>
                            <span class="info-value">{ip_address}</span>
                        </div>
        """ if ip_address else ""
        
        ip_text = f"IP Address: {ip_address}\n" if ip_address else ""
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #2d3748; background-color: #f7fafc; padding: 20px; }}
                .email-wrapper {{ max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }}
                .header {{ background: linear-gradient(135deg, #30cfd0 0%, #330867 100%); color: white; padding: 50px 30px; text-align: center; position: relative; }}
                .header::after {{ content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 4px; background: linear-gradient(90deg, rgba(255,255,255,0.3) 0%, rgba(255,255,255,0.1) 100%); }}
                .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 10px; letter-spacing: -0.5px; }}
                .header .icon {{ font-size: 48px; margin-bottom: 10px; }}
                .content {{ padding: 40px 35px; background-color: #ffffff; }}
                .greeting {{ font-size: 18px; color: #1a202c; margin-bottom: 20px; font-weight: 600; }}
                .message {{ font-size: 16px; color: #4a5568; margin-bottom: 25px; line-height: 1.8; }}
                .login-info {{ background: linear-gradient(135deg, #e0f7fa 0%, #b2ebf2 100%); border: 2px solid #30cfd0; padding: 30px; border-radius: 12px; margin: 30px 0; box-shadow: 0 2px 8px rgba(48, 207, 208, 0.15); }}
                .login-info h3 {{ font-size: 14px; color: #006064; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 20px; font-weight: 600; }}
                .info-item {{ display: flex; justify-content: space-between; padding: 15px 0; border-bottom: 1px solid rgba(48, 207, 208, 0.2); }}
                .info-item:last-child {{ border-bottom: none; }}
                .info-label {{ color: #006064; font-size: 14px; font-weight: 500; }}
                .info-value {{ color: #004d40; font-size: 16px; font-weight: 700; font-family: 'Courier New', monospace; }}
                .security-alert {{ background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); border-left: 4px solid #2196f3; padding: 20px; border-radius: 8px; margin: 30px 0; }}
                .security-alert .alert-icon {{ font-size: 24px; margin-bottom: 10px; }}
                .security-alert .alert-title {{ font-size: 16px; font-weight: 700; color: #1565c0; margin-bottom: 8px; }}
                .security-alert .alert-text {{ font-size: 14px; color: #0d47a1; line-height: 1.6; }}
                .button-container {{ text-align: center; margin: 35px 0; }}
                .button {{ display: inline-block; padding: 16px 40px; background: linear-gradient(135deg, #30cfd0 0%, #330867 100%); color: white; text-decoration: none; border-radius: 8px; font-weight: 600; font-size: 16px; box-shadow: 0 4px 12px rgba(48, 207, 208, 0.4); transition: transform 0.2s; }}
                .button:hover {{ transform: translateY(-2px); box-shadow: 0 6px 16px rgba(48, 207, 208, 0.5); }}
                .footer {{ background-color: #f7fafc; padding: 25px 35px; text-align: center; border-top: 1px solid #e2e8f0; }}
                .footer p {{ font-size: 12px; color: #a0aec0; margin: 5px 0; }}
                .footer .brand {{ color: #30cfd0; font-weight: 600; }}
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                <div class="header">
                    <div class="icon">🔐</div>
                    <h1>New Login Detected</h1>
                    <p>Security notification</p>
                </div>
                <div class="content">
                    <div class="greeting">Hello {user_name},</div>
                    <div class="message">
                        We detected a successful login to your FastCite account. This is a security notification to keep you informed about account activity.
                    </div>
                    <div class="login-info">
                        <h3>Login Details</h3>
                        <div class="info-item">
                            <span class="info-label">Login Time</span>
                            <span class="info-value">{login_time.strftime("%b %d, %Y %I:%M %p")}</span>
                        </div>
                        {ip_info}
                    </div>
                    <div class="security-alert">
                        <div class="alert-icon">⚠️</div>
                        <div class="alert-title">Security Notice</div>
                        <div class="alert-text">
                            If this wasn't you, please change your password immediately and contact our support team. Your account security may be at risk.
                        </div>
                    </div>
                    <div class="message">
                        If you recognize this login, you can safely ignore this email. We send these notifications to help keep your account secure.
                    </div>
                    <div class="button-container">
                        <a href="{FRONTEND_URL}/setting" class="button">Account Settings →</a>
                    </div>
                </div>
                <div class="footer">
                    <p>Best regards,<br><span class="brand">The FastCite Team</span></p>
                    <p style="margin-top: 15px;">This is an automated security notification. Please do not reply to this message.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        New Login Detected
        
        Hello {user_name},
        
        We detected a successful login to your FastCite account.
        
        Login Time: {login_time.strftime("%Y-%m-%d %H:%M:%S UTC")}
        {ip_text}
        ⚠️ Security Notice: If this wasn't you, please change your password immediately and contact our support team.
        
        Visit {FRONTEND_URL}/setting to manage your account settings.
        
        Best regards,
        The FastCite Team
        """
        
        return self._send_email(user_email, subject, html_body, text_body)
    
    def send_password_reset_code_email(self, user_email: str, user_name: str, reset_code: str):
        """Send password reset code email."""
        subject = "Password Reset Code 🔐"
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #2d3748; background-color: #f7fafc; padding: 20px; }}
                .email-wrapper {{ max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 50px 30px; text-align: center; position: relative; }}
                .header::after {{ content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 4px; background: linear-gradient(90deg, rgba(255,255,255,0.3) 0%, rgba(255,255,255,0.1) 100%); }}
                .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 10px; letter-spacing: -0.5px; }}
                .header .icon {{ font-size: 48px; margin-bottom: 10px; }}
                .content {{ padding: 40px 35px; background-color: #ffffff; }}
                .greeting {{ font-size: 18px; color: #1a202c; margin-bottom: 20px; font-weight: 600; }}
                .message {{ font-size: 16px; color: #4a5568; margin-bottom: 25px; line-height: 1.8; }}
                .code-box {{ background: linear-gradient(135deg, #f0f4ff 0%, #e0e7ff 100%); border: 3px solid #667eea; padding: 30px; border-radius: 12px; margin: 30px 0; text-align: center; box-shadow: 0 4px 12px rgba(102, 126, 234, 0.2); }}
                .code-box .code-label {{ font-size: 14px; color: #667eea; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 15px; font-weight: 600; }}
                .code-box .code-value {{ font-size: 42px; font-weight: 700; color: #4c1d95; letter-spacing: 8px; font-family: 'Courier New', monospace; }}
                .security-alert {{ background: linear-gradient(135deg, #fff5e6 0%, #ffe6cc 100%); border-left: 4px solid #ff9800; padding: 20px; border-radius: 8px; margin: 30px 0; }}
                .security-alert .alert-icon {{ font-size: 24px; margin-bottom: 10px; }}
                .security-alert .alert-title {{ font-size: 16px; font-weight: 700; color: #e65100; margin-bottom: 8px; }}
                .security-alert .alert-text {{ font-size: 14px; color: #bf360c; line-height: 1.6; }}
                .info-box {{ background: #f7fafc; border: 1px solid #e2e8f0; padding: 20px; border-radius: 8px; margin: 25px 0; }}
                .info-box .info-label {{ font-size: 12px; color: #718096; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 5px; }}
                .info-box .info-value {{ font-size: 16px; color: #2d3748; font-weight: 600; }}
                .footer {{ background-color: #f7fafc; padding: 25px 35px; text-align: center; border-top: 1px solid #e2e8f0; }}
                .footer p {{ font-size: 12px; color: #a0aec0; margin: 5px 0; }}
                .footer .brand {{ color: #667eea; font-weight: 600; }}
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                <div class="header">
                    <div class="icon">🔐</div>
                    <h1>Password Reset Code</h1>
                    <p>Use this code to reset your password</p>
                </div>
                <div class="content">
                    <div class="greeting">Hello {user_name},</div>
                    <div class="message">
                        You requested to reset your password. Use the code below to verify your identity and set a new password.
                    </div>
                    <div class="code-box">
                        <div class="code-label">Your Reset Code</div>
                        <div class="code-value">{reset_code}</div>
                    </div>
                    <div class="info-box">
                        <div class="info-label">Code Expires In</div>
                        <div class="info-value">10 minutes</div>
                    </div>
                    <div class="security-alert">
                        <div class="alert-icon">⚠️</div>
                        <div class="alert-title">Security Notice</div>
                        <div class="alert-text">
                            If you did not request this password reset, please ignore this email. Your account remains secure. Do not share this code with anyone.
                        </div>
                    </div>
                    <div class="message">
                        Enter this code in the password reset form to continue. The code will expire in 10 minutes for your security.
                    </div>
                </div>
                <div class="footer">
                    <p>Best regards,<br><span class="brand">The FastCite Team</span></p>
                    <p style="margin-top: 15px;">This is an automated security email. Please do not reply to this message.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        Password Reset Code
        
        Hello {user_name},
        
        You requested to reset your password. Use the code below to verify your identity:
        
        Reset Code: {reset_code}
        
        This code will expire in 10 minutes.
        
        ⚠️ Security Notice: If you did not request this password reset, please ignore this email. Your account remains secure.
        
        Best regards,
        The FastCite Team
        """
        
        return self._send_email(user_email, subject, html_body, text_body)
    
    def send_signup_verification_code_email(self, user_email: str, user_name: str, verification_code: str):
        """Send signup verification code email."""
        subject = "Verify Your FastCite Account 🎉"
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #2d3748; background-color: #f7fafc; padding: 20px; }}
                .email-wrapper {{ max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }}
                .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 50px 30px; text-align: center; position: relative; }}
                .header::after {{ content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 4px; background: linear-gradient(90deg, rgba(255,255,255,0.3) 0%, rgba(255,255,255,0.1) 100%); }}
                .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 10px; letter-spacing: -0.5px; }}
                .header .icon {{ font-size: 48px; margin-bottom: 10px; }}
                .content {{ padding: 40px 35px; background-color: #ffffff; }}
                .greeting {{ font-size: 18px; color: #1a202c; margin-bottom: 20px; font-weight: 600; }}
                .message {{ font-size: 16px; color: #4a5568; margin-bottom: 25px; line-height: 1.8; }}
                .code-box {{ background: linear-gradient(135deg, #f0f4ff 0%, #e0e7ff 100%); border: 3px solid #667eea; padding: 30px; border-radius: 12px; margin: 30px 0; text-align: center; box-shadow: 0 4px 12px rgba(102, 126, 234, 0.2); }}
                .code-box .code-label {{ font-size: 14px; color: #667eea; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 15px; font-weight: 600; }}
                .code-box .code-value {{ font-size: 42px; font-weight: 700; color: #4c1d95; letter-spacing: 8px; font-family: 'Courier New', monospace; }}
                .info-box {{ background: #f7fafc; border: 1px solid #e2e8f0; padding: 20px; border-radius: 8px; margin: 25px 0; }}
                .info-box .info-label {{ font-size: 12px; color: #718096; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 5px; }}
                .info-box .info-value {{ font-size: 16px; color: #2d3748; font-weight: 600; }}
                .footer {{ background-color: #f7fafc; padding: 25px 35px; text-align: center; border-top: 1px solid #e2e8f0; }}
                .footer p {{ font-size: 12px; color: #a0aec0; margin: 5px 0; }}
                .footer .brand {{ color: #667eea; font-weight: 600; }}
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                <div class="header">
                    <div class="icon">🎉</div>
                    <h1>Welcome to FastCite!</h1>
                    <p>Verify your email to complete signup</p>
                </div>
                <div class="content">
                    <div class="greeting">Hello {user_name},</div>
                    <div class="message">
                        Thank you for signing up for FastCite! To complete your registration, please verify your email address using the code below.
                    </div>
                    <div class="code-box">
                        <div class="code-label">Your Verification Code</div>
                        <div class="code-value">{verification_code}</div>
                    </div>
                    <div class="info-box">
                        <div class="info-label">Code Expires In</div>
                        <div class="info-value">10 minutes</div>
                    </div>
                    <div class="message">
                        Enter this code in the signup form to verify your email and activate your account. The code will expire in 10 minutes for your security.
                    </div>
                </div>
                <div class="footer">
                    <p>Best regards,<br><span class="brand">The FastCite Team</span></p>
                    <p style="margin-top: 15px;">This is an automated email. Please do not reply to this message.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        Welcome to FastCite!
        
        Hello {user_name},
        
        Thank you for signing up for FastCite! To complete your registration, please verify your email address.
        
        Verification Code: {verification_code}
        
        This code will expire in 10 minutes.
        
        Enter this code in the signup form to verify your email and activate your account.
        
        Best regards,
        The FastCite Team
        """
        
        return self._send_email(user_email, subject, html_body, text_body)

    def send_account_deletion_code_email(self, user_email: str, user_name: str, deletion_code: str):
        """Send account deletion verification code email."""
        subject = "Account Deletion Verification Code ⚠️"
        
        html_body = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
                * {{ margin: 0; padding: 0; box-sizing: border-box; }}
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #2d3748; background-color: #f7fafc; padding: 20px; }}
                .email-wrapper {{ max-width: 600px; margin: 0 auto; background-color: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); }}
                .header {{ background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%); color: white; padding: 50px 30px; text-align: center; position: relative; }}
                .header::after {{ content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 4px; background: linear-gradient(90deg, rgba(255,255,255,0.3) 0%, rgba(255,255,255,0.1) 100%); }}
                .header h1 {{ font-size: 32px; font-weight: 700; margin-bottom: 10px; letter-spacing: -0.5px; }}
                .header .icon {{ font-size: 48px; margin-bottom: 10px; }}
                .content {{ padding: 40px 35px; background-color: #ffffff; }}
                .greeting {{ font-size: 18px; color: #1a202c; margin-bottom: 20px; font-weight: 600; }}
                .message {{ font-size: 16px; color: #4a5568; margin-bottom: 25px; line-height: 1.8; }}
                .warning-box {{ background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%); border: 3px solid #ef4444; padding: 25px; border-radius: 12px; margin: 30px 0; box-shadow: 0 4px 12px rgba(239, 68, 68, 0.2); }}
                .warning-box .warning-title {{ font-size: 18px; color: #dc2626; font-weight: 700; margin-bottom: 10px; }}
                .warning-box .warning-text {{ font-size: 14px; color: #991b1b; line-height: 1.6; }}
                .code-box {{ background: linear-gradient(135deg, #f0f4ff 0%, #e0e7ff 100%); border: 3px solid #667eea; padding: 30px; border-radius: 12px; margin: 30px 0; text-align: center; box-shadow: 0 4px 12px rgba(102, 126, 234, 0.2); }}
                .code-box .code-label {{ font-size: 14px; color: #667eea; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 15px; font-weight: 600; }}
                .code-box .code-value {{ font-size: 42px; font-weight: 700; color: #4c1d95; letter-spacing: 8px; font-family: 'Courier New', monospace; }}
                .info-box {{ background: #f7fafc; border: 1px solid #e2e8f0; padding: 20px; border-radius: 8px; margin: 25px 0; }}
                .info-box .info-label {{ font-size: 12px; color: #718096; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 5px; }}
                .info-box .info-value {{ font-size: 16px; color: #2d3748; font-weight: 600; }}
                .footer {{ background-color: #f7fafc; padding: 25px 35px; text-align: center; border-top: 1px solid #e2e8f0; }}
                .footer p {{ font-size: 12px; color: #a0aec0; margin: 5px 0; }}
                .footer .brand {{ color: #667eea; font-weight: 600; }}
            </style>
        </head>
        <body>
            <div class="email-wrapper">
                <div class="header">
                    <div class="icon">⚠️</div>
                    <h1>Account Deletion Request</h1>
                    <p>Verify your identity to proceed</p>
                </div>
                <div class="content">
                    <div class="greeting">Hello {user_name},</div>
                    <div class="message">
                        We received a request to delete your FastCite account. To confirm this action, please use the verification code below.
                    </div>
                    <div class="warning-box">
                        <div class="warning-title">⚠️ Important Warning</div>
                        <div class="warning-text">
                            This action is permanent and cannot be undone. All your data, including books, chat sessions, and account information, will be permanently deleted.
                        </div>
                    </div>
                    <div class="code-box">
                        <div class="code-label">Your Deletion Verification Code</div>
                        <div class="code-value">{deletion_code}</div>
                    </div>
                    <div class="info-box">
                        <div class="info-label">Code Expires In</div>
                        <div class="info-value">10 minutes</div>
                    </div>
                    <div class="message">
                        Enter this code in the account deletion form to proceed. If you did not request this, please ignore this email and your account will remain safe.
                    </div>
                </div>
                <div class="footer">
                    <p>Best regards,<br><span class="brand">The FastCite Team</span></p>
                    <p style="margin-top: 15px;">This is an automated email. Please do not reply to this message.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        text_body = f"""
        Account Deletion Request
        
        Hello {user_name},
        
        We received a request to delete your FastCite account. To confirm this action, please use the verification code below.
        
        ⚠️ Important Warning: This action is permanent and cannot be undone. All your data, including books, chat sessions, and account information, will be permanently deleted.
        
        Deletion Verification Code: {deletion_code}
        
        This code will expire in 10 minutes.
        
        Enter this code in the account deletion form to proceed. If you did not request this, please ignore this email and your account will remain safe.
        
        Best regards,
        The FastCite Team
        """
        
        return self._send_email(user_email, subject, html_body, text_body)


# Global email service instance
email_service = EmailService()

