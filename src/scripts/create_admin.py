#!/usr/bin/env python3
"""
Script to create an admin user in the FastCite database.

Usage:
    python create_admin.py
    python create_admin.py --username admin --email admin@example.com --name "Admin User" --password mypassword
    python create_admin.py -u admin -e admin@example.com -n "Admin User" -p mypassword
"""

import sys
import os
from pathlib import Path
from uuid import uuid4
from datetime import datetime
import getpass
import argparse

# Add parent directory to path to import modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.mongo import users_collections
from passlib.context import CryptContext

# Password Hashing (Argon2) - same as auth.py
pwd_context = CryptContext(schemes=["argon2"], deprecated="auto")

def hash_password(password: str) -> str:
    """Hash password using Argon2."""
    return pwd_context.hash(password)

def create_admin_user(username: str, email: str, name: str, password: str):
    """
    Create an admin user in the database.
    
    Args:
        username: Username for the admin
        email: Email address for the admin
        name: Full name of the admin
        password: Plain text password (will be hashed)
    
    Returns:
        dict: Result with status and message
    """
    try:
        # Check if user already exists
        existing_user = users_collections.find_one({
            "$or": [
                {"username": username},
                {"email": email}
            ]
        })
        
        if existing_user:
            return {
                "success": False,
                "message": f"User already exists with username '{username}' or email '{email}'"
            }
        
        # Create admin user
        admin_user = {
            "id": str(uuid4()),
            "username": username,
            "pass_hash": hash_password(password),
            "name": name,
            "email": email,
            "role": "admin",
            "created_at": datetime.utcnow(),
        }
        
        # Insert into database
        result = users_collections.insert_one(admin_user)
        
        if result.inserted_id:
            return {
                "success": True,
                "message": f"Admin user '{username}' created successfully!",
                "user_id": admin_user["id"]
            }
        else:
            return {
                "success": False,
                "message": "Failed to create admin user"
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": f"Error creating admin user: {str(e)}"
        }

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(
        description="Create an admin user in FastCite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode (will prompt for all fields)
  python create_admin.py
  
  # Command line mode
  python create_admin.py --username admin --email admin@example.com --name "Admin User" --password mypassword
  
  # Short form
  python create_admin.py -u admin -e admin@example.com -n "Admin User" -p mypassword
        """
    )
    
    parser.add_argument(
        "-u", "--username",
        help="Username for the admin user"
    )
    parser.add_argument(
        "-e", "--email",
        help="Email address for the admin user"
    )
    parser.add_argument(
        "-n", "--name",
        help="Full name of the admin user"
    )
    parser.add_argument(
        "-p", "--password",
        help="Password for the admin user (not recommended: use interactive mode for security)"
    )
    
    args = parser.parse_args()
    
    # Interactive mode if arguments not provided
    if not args.username:
        print("=" * 60)
        print("FastCite Admin User Creation Script")
        print("=" * 60)
        print()
        
        username = input("Enter username: ").strip()
        if not username:
            print("❌ Username is required!")
            sys.exit(1)
        
        email = input("Enter email: ").strip()
        if not email:
            print("❌ Email is required!")
            sys.exit(1)
        
        name = input("Enter full name: ").strip()
        if not name:
            print("❌ Name is required!")
            sys.exit(1)
        
        password = getpass.getpass("Enter password: ").strip()
        if not password:
            print("❌ Password is required!")
            sys.exit(1)
        
        confirm_password = getpass.getpass("Confirm password: ").strip()
        if password != confirm_password:
            print("❌ Passwords do not match!")
            sys.exit(1)
        
        if len(password) < 8:
            print("❌ Password must be at least 8 characters long!")
            sys.exit(1)
    else:
        # Command line mode
        username = args.username
        email = args.email
        name = args.name
        password = args.password
        
        # Validate required fields
        if not all([username, email, name, password]):
            print("❌ All fields (username, email, name, password) are required!")
            parser.print_help()
            sys.exit(1)
        
        if len(password) < 8:
            print("❌ Password must be at least 8 characters long!")
            sys.exit(1)
    
    # Create admin user
    print("\nCreating admin user...")
    result = create_admin_user(username, email, name, password)
    
    if result["success"]:
        print(f"✅ {result['message']}")
        print(f"   User ID: {result.get('user_id', 'N/A')}")
        print(f"   Username: {username}")
        print(f"   Email: {email}")
        print(f"   Role: admin")
        sys.exit(0)
    else:
        print(f"❌ {result['message']}")
        sys.exit(1)

if __name__ == "__main__":
    main()

