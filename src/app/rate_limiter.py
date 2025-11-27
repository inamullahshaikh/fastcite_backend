"""
Rate limiter for Gemini API calls to prevent exceeding 15 RPM limit.
Uses Redis to track requests per minute with a sliding window.
"""
import os
import time
from typing import Optional
from datetime import datetime, timedelta
import redis
from dotenv import load_dotenv

load_dotenv()

# Redis connection for rate limiting
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
RATE_LIMIT_REDIS_DB = 1  # Use separate DB for rate limiting

# Rate limit configuration
GEMINI_RPM_LIMIT = 15  # Requests per minute limit
GEMINI_RPM_SAFE_THRESHOLD = 14  # Stop at 14 to prevent hitting 15

# Global Redis client for rate limiting
_rate_limiter_redis: Optional[redis.Redis] = None


def get_rate_limiter_redis() -> Optional[redis.Redis]:
    """Get or create Redis client for rate limiting."""
    global _rate_limiter_redis
    if _rate_limiter_redis is None:
        try:
            # Try to use Redis URL directly first (for Redis 4.0+)
            try:
                _rate_limiter_redis = redis.from_url(
                    REDIS_URL,
                    db=RATE_LIMIT_REDIS_DB,
                    decode_responses=True,
                    socket_connect_timeout=2,
                    socket_timeout=2
                )
                # Test connection
                _rate_limiter_redis.ping()
                print(f"✅ Rate limiter Redis connected via URL (DB {RATE_LIMIT_REDIS_DB})")
            except Exception:
                # Fallback: Parse Redis URL manually
                if REDIS_URL.startswith("redis://"):
                    # Extract host, port, and db from URL
                    url_parts = REDIS_URL.replace("redis://", "").split("/")
                    host_port = url_parts[0].split(":")
                    host = host_port[0] if len(host_port) > 0 else "localhost"
                    port = int(host_port[1]) if len(host_port) > 1 else 6379
                else:
                    host = "localhost"
                    port = 6379
                
                _rate_limiter_redis = redis.Redis(
                    host=host,
                    port=port,
                    db=RATE_LIMIT_REDIS_DB,
                    decode_responses=True,
                    socket_connect_timeout=2,
                    socket_timeout=2
                )
                # Test connection
                _rate_limiter_redis.ping()
                print(f"✅ Rate limiter Redis connected: {host}:{port}/{RATE_LIMIT_REDIS_DB}")
        except Exception as e:
            print(f"⚠️ Failed to connect to Redis for rate limiting: {e}")
            print("⚠️ Rate limiting will use in-memory fallback (not shared across workers)")
            _rate_limiter_redis = None
    
    return _rate_limiter_redis


# In-memory fallback for rate limiting (if Redis unavailable)
_in_memory_requests = []


def check_rate_limit() -> tuple[bool, Optional[float]]:
    """
    Check if we can make a Gemini API call based on rate limits.
    
    Returns:
        tuple: (can_proceed: bool, wait_seconds: Optional[float])
        - can_proceed: True if we can make the call, False if we should wait
        - wait_seconds: How long to wait before retrying (None if can_proceed)
    """
    redis_client = get_rate_limiter_redis()
    current_time = time.time()
    window_start = current_time - 60  # Last 60 seconds
    
    if redis_client:
        # Use Redis for distributed rate limiting
        try:
            # Use a sorted set to store request timestamps
            key = "gemini:requests"
            
            # Remove old entries (outside the 1-minute window)
            redis_client.zremrangebyscore(key, 0, window_start)
            
            # Count requests in the current window
            request_count = redis_client.zcard(key)
            
            # Check if we're at or above the safe threshold
            if request_count >= GEMINI_RPM_SAFE_THRESHOLD:
                # Get the oldest request timestamp in the window
                oldest_requests = redis_client.zrange(key, 0, 0, withscores=True)
                if oldest_requests:
                    oldest_timestamp = oldest_requests[0][1]
                    # Calculate how long to wait until the oldest request expires
                    wait_seconds = (oldest_timestamp + 60) - current_time
                    if wait_seconds > 0:
                        print(f"⏸️ Rate limit reached: {request_count}/{GEMINI_RPM_SAFE_THRESHOLD} requests in last minute. Wait {wait_seconds:.1f}s")
                        return False, wait_seconds
                    else:
                        # Oldest request expired, we can proceed
                        pass
            
            # Add current request timestamp
            redis_client.zadd(key, {str(current_time): current_time})
            # Set expiration on the key (cleanup after 2 minutes)
            redis_client.expire(key, 120)
            
            print(f"✅ Rate limit check: {request_count + 1}/{GEMINI_RPM_SAFE_THRESHOLD} requests in last minute")
            return True, None
            
        except Exception as e:
            print(f"⚠️ Redis rate limiting error: {e}, falling back to in-memory")
            # Fall through to in-memory fallback
    
    # In-memory fallback (not shared across workers, but better than nothing)
    global _in_memory_requests
    
    # Remove old entries
    _in_memory_requests = [ts for ts in _in_memory_requests if ts > window_start]
    
    # Check if we're at or above the safe threshold
    if len(_in_memory_requests) >= GEMINI_RPM_SAFE_THRESHOLD:
        # Get the oldest request timestamp
        oldest_timestamp = min(_in_memory_requests)
        # Calculate how long to wait
        wait_seconds = (oldest_timestamp + 60) - current_time
        if wait_seconds > 0:
            print(f"⏸️ Rate limit reached (in-memory): {len(_in_memory_requests)}/{GEMINI_RPM_SAFE_THRESHOLD} requests. Wait {wait_seconds:.1f}s")
            return False, wait_seconds
    
    # Add current request
    _in_memory_requests.append(current_time)
    print(f"✅ Rate limit check (in-memory): {len(_in_memory_requests)}/{GEMINI_RPM_SAFE_THRESHOLD} requests")
    return True, None


def record_api_call():
    """
    Record that a Gemini API call was made.
    This is called after a successful API call.
    """
    redis_client = get_rate_limiter_redis()
    current_time = time.time()
    
    if redis_client:
        try:
            key = "gemini:requests"
            redis_client.zadd(key, {str(current_time): current_time})
            redis_client.expire(key, 120)
        except Exception as e:
            print(f"⚠️ Failed to record API call in Redis: {e}")
    
    # Also update in-memory (for fallback)
    global _in_memory_requests
    _in_memory_requests.append(current_time)
    # Clean old entries
    window_start = current_time - 60
    _in_memory_requests = [ts for ts in _in_memory_requests if ts > window_start]

