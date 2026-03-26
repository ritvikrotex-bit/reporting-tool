"""
db.py — SQLite user auth + MT5 credential storage for MT5 P&L Studio.

Tables:
  app_users    — app login credentials (SHA-256 hashed passwords)
  mt5_profiles — saved MT5 connection configs per user (Fernet-encrypted MT5 passwords)
"""

import sqlite3
import hashlib
import base64
import os
from typing import Optional

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "users.db")


# ── Encryption helpers ────────────────────────────────────────────────────────

def _fernet_key() -> Optional[bytes]:
    """Derive a deterministic Fernet key from PBKDF2 over DB_PATH. Returns None if cryptography unavailable."""
    try:
        from cryptography.fernet import Fernet
        import hashlib
        raw = hashlib.pbkdf2_hmac(
            "sha256",
            b"mt5studio_v1_salt",
            DB_PATH.encode(),
            100_000,
        )
        return base64.urlsafe_b64encode(raw)
    except ImportError:
        return None


def _encrypt(plaintext: str) -> str:
    key = _fernet_key()
    if key:
        from cryptography.fernet import Fernet
        return "f:" + Fernet(key).encrypt(plaintext.encode()).decode()
    return "b:" + base64.b64encode(plaintext.encode()).decode()


def _decrypt(stored: str) -> str:
    if stored.startswith("f:"):
        key = _fernet_key()
        if key:
            from cryptography.fernet import Fernet
            return Fernet(key).decrypt(stored[2:].encode()).decode()
    if stored.startswith("b:"):
        return base64.b64decode(stored[2:]).decode()
    return stored  # legacy plain fallback


# ── Password hashing ──────────────────────────────────────────────────────────

def _hash_pw(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


# ── DB connection helper ──────────────────────────────────────────────────────

def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con


# ── Initialisation ────────────────────────────────────────────────────────────

def init_db() -> None:
    """Create tables if missing. Seed default admin on first run."""
    with _conn() as con:
        con.executescript("""
            CREATE TABLE IF NOT EXISTS app_users (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                username        TEXT    NOT NULL UNIQUE,
                password_hash   TEXT    NOT NULL,
                is_admin        INTEGER NOT NULL DEFAULT 0,
                must_change_pw  INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT    NOT NULL DEFAULT (datetime('now'))
            );
            CREATE TABLE IF NOT EXISTS mt5_profiles (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id          INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
                name             TEXT    NOT NULL,
                server           TEXT    NOT NULL,
                mt5_login        INTEGER NOT NULL,
                mt5_password_enc TEXT    NOT NULL,
                created_at       TEXT    NOT NULL DEFAULT (datetime('now')),
                UNIQUE(user_id, name)
            );
        """)
        row = con.execute("SELECT COUNT(*) FROM app_users").fetchone()[0]
        if row == 0:
            con.execute(
                "INSERT INTO app_users (username, password_hash, is_admin, must_change_pw) VALUES (?,?,1,1)",
                ("admin", _hash_pw("admin")),
            )


# ── App user operations ───────────────────────────────────────────────────────

def verify_user(username: str, password: str) -> Optional[dict]:
    """Return user dict on success, None on failure."""
    with _conn() as con:
        row = con.execute(
            "SELECT id, username, is_admin, must_change_pw FROM app_users WHERE username=? AND password_hash=?",
            (username.strip(), _hash_pw(password)),
        ).fetchone()
    return dict(row) if row else None


def change_password(user_id: int, new_password: str) -> None:
    with _conn() as con:
        con.execute(
            "UPDATE app_users SET password_hash=?, must_change_pw=0 WHERE id=?",
            (_hash_pw(new_password), user_id),
        )


def create_user(username: str, password: str, is_admin: int = 0) -> None:
    """Raises ValueError if username already exists."""
    try:
        with _conn() as con:
            con.execute(
                "INSERT INTO app_users (username, password_hash, is_admin) VALUES (?,?,?)",
                (username.strip(), _hash_pw(password), is_admin),
            )
    except sqlite3.IntegrityError:
        raise ValueError(f"Username '{username}' already exists.")


def list_users() -> list:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, username, is_admin, must_change_pw, created_at FROM app_users ORDER BY id"
        ).fetchall()
    return [dict(r) for r in rows]


def delete_user(user_id: int) -> None:
    with _conn() as con:
        con.execute("DELETE FROM app_users WHERE id=?", (user_id,))


# ── MT5 profile operations ────────────────────────────────────────────────────

def get_mt5_profiles(user_id: int) -> list:
    with _conn() as con:
        rows = con.execute(
            "SELECT id, name, server, mt5_login FROM mt5_profiles WHERE user_id=? ORDER BY name",
            (user_id,),
        ).fetchall()
    return [dict(r) for r in rows]


def get_mt5_profile_decrypted(profile_id: int, user_id: int) -> Optional[dict]:
    with _conn() as con:
        row = con.execute(
            "SELECT id, name, server, mt5_login, mt5_password_enc FROM mt5_profiles WHERE id=? AND user_id=?",
            (profile_id, user_id),
        ).fetchone()
    if not row:
        return None
    d = dict(row)
    d["mt5_password"] = _decrypt(d.pop("mt5_password_enc"))
    return d


def save_mt5_profile(user_id: int, name: str, server: str, mt5_login: int, mt5_password: str) -> None:
    """Insert or replace a profile (upsert by user_id + name)."""
    enc = _encrypt(mt5_password)
    with _conn() as con:
        con.execute(
            """INSERT INTO mt5_profiles (user_id, name, server, mt5_login, mt5_password_enc)
               VALUES (?,?,?,?,?)
               ON CONFLICT(user_id, name) DO UPDATE SET
                   server=excluded.server,
                   mt5_login=excluded.mt5_login,
                   mt5_password_enc=excluded.mt5_password_enc""",
            (user_id, name.strip(), server.strip(), int(mt5_login), enc),
        )


def delete_mt5_profile(profile_id: int, user_id: int) -> None:
    with _conn() as con:
        con.execute(
            "DELETE FROM mt5_profiles WHERE id=? AND user_id=?",
            (profile_id, user_id),
        )
