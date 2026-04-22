"""
backend/auth/routes.py
=======================
Authentication API routes: signup, login, /me.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.orm import Session

from database import get_db
from models import User
from schemas import UserCreate, UserResponse, LoginRequest, TokenResponse
from auth.jwt import hash_password, verify_password, create_access_token, decode_access_token

router = APIRouter(prefix="/auth", tags=["Authentication"])

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

# ── In-memory user cache: token → User dict (avoids DB lookup per request) ───
import threading as _threading
_user_cache: dict = {}
_user_cache_lock = _threading.Lock()
_USER_CACHE_TTL = 300  # 5 minutes


def _get_cached_user(token: str):
    import time
    with _user_cache_lock:
        entry = _user_cache.get(token)
        if entry and time.time() < entry["exp"]:
            return entry["user"]
        if entry:
            del _user_cache[token]
    return None


def _set_cached_user(token: str, user: User):
    import time
    with _user_cache_lock:
        # Keep cache bounded (max 200 entries)
        if len(_user_cache) > 200:
            oldest = min(_user_cache, key=lambda k: _user_cache[k]["exp"])
            del _user_cache[oldest]
        _user_cache[token] = {"user": user, "exp": time.time() + _USER_CACHE_TTL}


def invalidate_user_cache(email: str):
    with _user_cache_lock:
        to_del = [k for k, v in _user_cache.items() if getattr(v.get("user"), "email", None) == email]
        for k in to_del:
            del _user_cache[k]


# ── Dependency: get current user from Bearer token ────────────────────────────
def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: Session = Depends(get_db),
) -> User:
    """Validates JWT and returns User. Caches user object for 5 min to skip DB lookup."""
    cached = _get_cached_user(token)
    if cached is not None:
        return cached

    email = decode_access_token(token)
    if not email:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    _set_cached_user(token, user)
    return user


# ── POST /auth/signup ─────────────────────────────────────────────────────────
@router.post("/signup", response_model=TokenResponse, status_code=201)
def signup(payload: UserCreate, db: Session = Depends(get_db)):
    """Create a new user account and return a JWT token."""
    existing = db.query(User).filter(User.email == payload.email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")

    user = User(
        email           = payload.email,
        full_name       = payload.full_name,
        hashed_password = hash_password(payload.password),
        is_admin        = payload.is_admin or False,
        allowed_plants  = payload.allowed_plants,
    )
    db.add(user)
    db.commit()
    db.refresh(user)

    token = create_access_token({"sub": user.email})
    return TokenResponse(
        access_token=token,
        user=UserResponse.model_validate(user),
    )


# ── POST /auth/login ──────────────────────────────────────────────────────────
@router.post("/login", response_model=TokenResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    """Authenticate user and return JWT token."""
    user = db.query(User).filter(User.email == payload.email).first()
    if not user or not verify_password(payload.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
        )

    token = create_access_token({"sub": user.email})
    return TokenResponse(
        access_token=token,
        user=UserResponse.model_validate(user),
    )


# ── GET /auth/me ──────────────────────────────────────────────────────────────
@router.get("/me", response_model=UserResponse)
def get_me(current_user: User = Depends(get_current_user)):
    """Return the current authenticated user's profile."""
    return current_user
