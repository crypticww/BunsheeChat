import asyncio
import base64
import datetime
import hashlib
import hmac
import json
import math
import mimetypes
import os
import secrets
import smtplib
import ssl
import sqlite3
import time
from email.message import EmailMessage
from urllib.error import URLError
from urllib.request import urlopen

import websockets

clients = {}  # ws -> username

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
FROM_EMAIL = os.getenv("FROM_EMAIL", SMTP_USER)

# ── Rate limiting ─────────────────────────────────────────────────────────────
# 20 MB/s per connection
RATE_LIMIT_BYTES_PER_SEC = 20 * 1024 * 1024  # 20 MB/s
# Max file size: 2 GB
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024

# Pending file transfers: transfer_id -> {data, meta, received_chunks, total_chunks}
pending_transfers = {}
# Per-connection bandwidth tracking: ws -> {bytes_this_second, window_start}
bandwidth_tracker = {}

conn = sqlite3.connect("chat.db", check_same_thread=False)
conn.row_factory = sqlite3.Row
# Optimize SQLite for performance
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")
conn.execute("PRAGMA cache_size=10000")
conn.execute("PRAGMA temp_store=MEMORY")
cursor = conn.cursor()


def debug(msg):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def ensure_column(table, column_definition):
    column_name = column_definition.split()[0]
    cursor.execute(f"PRAGMA table_info({table})")
    existing = {row["name"] for row in cursor.fetchall()}
    if column_name not in existing:
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column_definition}")


def hash_password(password):
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200000).hex()
    return f"{salt.hex()}${digest}"


def verify_password(password, stored):
    if not stored:
        return False
    if "$" in stored:
        salt_hex, digest = stored.split("$", 1)
        try:
            salt = bytes.fromhex(salt_hex)
        except Exception:
            return False
        candidate = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, 200000).hex()
        return hmac.compare_digest(candidate, digest)
    return hmac.compare_digest(password, stored)


def init_db():
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            username TEXT PRIMARY KEY,
            email TEXT,
            password TEXT,
            last_device_id TEXT,
            last_device_name TEXT,
            last_ip TEXT,
            last_login_at TEXT,
            display_name TEXT,
            profile_picture TEXT,
            banner TEXT,
            bio TEXT,
            created_at TEXT
        )
    """)

    # Ensure all columns exist
    for col in ["email TEXT", "password TEXT", "last_device_id TEXT", "last_device_name TEXT",
                "last_ip TEXT", "last_login_at TEXT", "display_name TEXT", "profile_picture TEXT",
                "banner TEXT", "bio TEXT", "created_at TEXT"]:
        ensure_column("users", col)

    try:
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email ON users(email)")
    except Exception:
        pass

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender TEXT,
            receiver TEXT,
            message TEXT,
            msg_type TEXT DEFAULT 'text',
            file_name TEXT,
            file_size INTEGER,
            file_mime TEXT,
            file_data TEXT,
            timestamp TEXT
        )
    """)

    for col in ["msg_type TEXT DEFAULT 'text'", "file_name TEXT", "file_size INTEGER",
                "file_mime TEXT", "file_data TEXT", "timestamp TEXT"]:
        ensure_column("messages", col)

    # Add index for faster chat history queries
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_conv ON messages(sender, receiver)")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS conversations (
            user1 TEXT,
            user2 TEXT,
            last_message TEXT,
            last_message_at TEXT,
            UNIQUE(user1, user2)
        )
    """)
    for col in ["last_message TEXT", "last_message_at TEXT"]:
        ensure_column("conversations", col)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS blocked_users (
            blocker TEXT,
            blocked TEXT,
            blocked_at TEXT,
            UNIQUE(blocker, blocked)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            token TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            device_id TEXT,
            device_name TEXT,
            created_at TEXT,
            expires_at TEXT
        )
    """)

    conn.commit()
    debug("Database initialized")


# ── User helpers ──────────────────────────────────────────────────────────────

def user_exists_by_username(username):
    cursor.execute("SELECT 1 FROM users WHERE username=?", (username,))
    return cursor.fetchone() is not None


def user_exists_by_email(email):
    cursor.execute("SELECT 1 FROM users WHERE email=?", (email,))
    return cursor.fetchone() is not None


def get_user_by_identifier(identifier):
    cursor.execute(
        "SELECT * FROM users WHERE username=? OR email=? LIMIT 1",
        (identifier, identifier)
    )
    return cursor.fetchone()


def create_user(email, username, password, device_id, device_name, ip):
    if user_exists_by_username(username):
        return False, "Username already exists"
    if user_exists_by_email(email):
        return False, "Email already exists"
    try:
        now = datetime.datetime.utcnow().isoformat()
        cursor.execute("""
            INSERT INTO users (username, email, password, last_device_id, last_device_name,
                               last_ip, last_login_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (username, email, hash_password(password), device_id, device_name, ip, now, now))
        conn.commit()
        return True, None
    except sqlite3.IntegrityError:
        return False, "Email already exists"
    except Exception as e:
        return False, "Registration failed"


def update_login_meta(username, device_id, device_name, ip):
    cursor.execute("""
        UPDATE users SET last_device_id=?, last_device_name=?, last_ip=?, last_login_at=?
        WHERE username=?
    """, (device_id, device_name, ip, datetime.datetime.utcnow().isoformat(), username))
    conn.commit()


def create_session(username, device_id, device_name):
    token = secrets.token_hex(32)
    now = datetime.datetime.utcnow()
    expires = now + datetime.timedelta(days=30)
    cursor.execute("""
        INSERT INTO sessions (token, username, device_id, device_name, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (token, username, device_id, device_name, now.isoformat(), expires.isoformat()))
    conn.commit()
    return token


def validate_session(token):
    if not token:
        return None
    cursor.execute("SELECT * FROM sessions WHERE token=?", (token,))
    row = cursor.fetchone()
    if not row:
        return None
    try:
        expires = datetime.datetime.fromisoformat(row["expires_at"])
        if datetime.datetime.utcnow() > expires:
            cursor.execute("DELETE FROM sessions WHERE token=?", (token,))
            conn.commit()
            return None
    except Exception:
        return None
    return row


def delete_session(token):
    if token:
        cursor.execute("DELETE FROM sessions WHERE token=?", (token,))
        conn.commit()


def save_message(sender, receiver, message, msg_type="text",
                 file_name=None, file_size=None, file_mime=None, file_data=None):
    ts = datetime.datetime.utcnow().isoformat()
    cursor.execute("""
        INSERT INTO messages (sender, receiver, message, msg_type, file_name, file_size,
                              file_mime, file_data, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (sender, receiver, message, msg_type, file_name, file_size, file_mime, file_data, ts))
    conn.commit()
    return cursor.lastrowid, ts


def save_conversation(user1, user2, last_message=""):
    a, b = sorted([user1, user2])
    now = datetime.datetime.utcnow().isoformat()
    cursor.execute("""
        INSERT INTO conversations (user1, user2, last_message, last_message_at)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user1, user2) DO UPDATE SET
            last_message=excluded.last_message,
            last_message_at=excluded.last_message_at
    """, (a, b, last_message, now))
    conn.commit()


def get_chats(user):
    cursor.execute("""
        SELECT c.user1, c.user2, c.last_message, c.last_message_at
        FROM conversations c
        WHERE c.user1=? OR c.user2=?
        ORDER BY c.last_message_at DESC NULLS LAST
    """, (user, user))
    rows = cursor.fetchall()
    result = []
    for row in rows:
        other = row["user2"] if row["user1"] == user else row["user1"]
        cursor.execute(
            "SELECT display_name, profile_picture, banner, bio, created_at FROM users WHERE username=?",
            (other,)
        )
        u = cursor.fetchone()
        result.append({
            "username": other,
            "display_name": u["display_name"] if u else None,
            "profile_picture": u["profile_picture"] if u else None,
            "banner": u["banner"] if u else None,
            "bio": u["bio"] if u else None,
            "created_at": u["created_at"] if u else None,
            "last_message": row["last_message"],
            "last_message_at": row["last_message_at"],
        })
    return result


def get_chat_history(user1, user2, limit=100, offset=0):
    cursor.execute("""
        SELECT id, sender, message, msg_type, file_name, file_size, file_mime, file_data, timestamp
        FROM messages
        WHERE (sender=? AND receiver=?) OR (sender=? AND receiver=?)
        ORDER BY id ASC
        LIMIT ? OFFSET ?
    """, (user1, user2, user2, user1, limit, offset))
    rows = cursor.fetchall()
    result = []
    for r in rows:
        item = {
            "id": r["id"],
            "sender": r["sender"],
            "message": r["message"],
            "type": r["msg_type"] or "text",
            "timestamp": r["timestamp"],
        }
        if r["msg_type"] and r["msg_type"] != "text":
            item["file_name"] = r["file_name"]
            item["file_size"] = r["file_size"]
            item["file_mime"] = r["file_mime"]
            item["file_data"] = r["file_data"]
        result.append(item)
    return result


def get_user_profile(username):
    cursor.execute(
        "SELECT username, email, display_name, profile_picture, banner, bio, created_at FROM users WHERE username=?",
        (username,)
    )
    return cursor.fetchone()


def update_field(username, field, value):
    # Only allow safe field names
    allowed = {"display_name", "profile_picture", "banner", "bio"}
    if field not in allowed:
        return
    cursor.execute(f"UPDATE users SET {field}=? WHERE username=?", (value, username))
    conn.commit()


def block_user(blocker, blocked):
    try:
        cursor.execute(
            "INSERT INTO blocked_users (blocker, blocked, blocked_at) VALUES (?, ?, ?)",
            (blocker, blocked, datetime.datetime.utcnow().isoformat())
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False


def unblock_user(blocker, blocked):
    cursor.execute("DELETE FROM blocked_users WHERE blocker=? AND blocked=?", (blocker, blocked))
    conn.commit()
    return cursor.rowcount > 0


def is_blocked(blocker, blocked):
    cursor.execute("SELECT 1 FROM blocked_users WHERE blocker=? AND blocked=?", (blocker, blocked))
    return cursor.fetchone() is not None


def get_blocked_users(blocker):
    cursor.execute("SELECT blocked FROM blocked_users WHERE blocker=?", (blocker,))
    return [row["blocked"] for row in cursor.fetchall()]


def get_users_matching(query, exclude):
    cursor.execute("""
        SELECT username, display_name, profile_picture FROM users
        WHERE LOWER(username) LIKE ? AND username != ?
        ORDER BY username ASC LIMIT 20
    """, (f"%{query.lower()}%", exclude))
    return [{"username": r["username"], "display_name": r["display_name"],
             "profile_picture": r["profile_picture"]} for r in cursor.fetchall()]


# ── Rate limiting helpers ─────────────────────────────────────────────────────

async def rate_limited_send(ws, data_bytes):
    """Send data respecting 20 MB/s rate limit per connection."""
    if ws not in bandwidth_tracker:
        bandwidth_tracker[ws] = {"bytes": 0, "window": time.monotonic()}

    tracker = bandwidth_tracker[ws]
    now = time.monotonic()
    elapsed = now - tracker["window"]

    if elapsed >= 1.0:
        tracker["bytes"] = 0
        tracker["window"] = now

    chunk_size = RATE_LIMIT_BYTES_PER_SEC
    offset = 0
    total = len(data_bytes)

    while offset < total:
        remaining_budget = RATE_LIMIT_BYTES_PER_SEC - tracker["bytes"]
        if remaining_budget <= 0:
            # Wait for next window
            wait = 1.0 - (time.monotonic() - tracker["window"])
            if wait > 0:
                await asyncio.sleep(wait)
            tracker["bytes"] = 0
            tracker["window"] = time.monotonic()
            remaining_budget = RATE_LIMIT_BYTES_PER_SEC

        send_now = min(remaining_budget, total - offset)
        await ws.send(data_bytes[offset:offset + send_now])
        tracker["bytes"] += send_now
        offset += send_now


# ── Networking helpers ────────────────────────────────────────────────────────

def is_private_ip(ip):
    if ip in {"127.0.0.1", "::1", "", None}:
        return True
    for prefix in ("10.", "192.168.", "172.16.", "172.17.", "172.18.", "172.19.",
                   "172.2", "172.30.", "172.31."):
        if ip.startswith(prefix):
            return True
    return False


def get_location_from_ip(ip):
    if is_private_ip(ip):
        return "Unknown"
    try:
        with urlopen(f"https://ipapi.co/{ip}/json/", timeout=3) as response:
            data = json.loads(response.read().decode("utf-8"))
        parts = [p for p in [data.get("city"), data.get("region"), data.get("country_name")] if p]
        return ", ".join(parts) if parts else "Unknown"
    except Exception:
        return "Unknown"


def send_email(to_email, subject, body):
    if not SMTP_HOST or not SMTP_USER or not SMTP_PASS:
        return False
    msg = EmailMessage()
    msg["From"] = FROM_EMAIL
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.set_content(body)
    try:
        ctx = ssl.create_default_context()
        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx) as srv:
                srv.login(SMTP_USER, SMTP_PASS)
                srv.send_message(msg)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as srv:
                srv.starttls(context=ctx)
                srv.login(SMTP_USER, SMTP_PASS)
                srv.send_message(msg)
        return True
    except Exception as e:
        debug(f"Email error: {e}")
        return False


async def send_to_user(username, payload):
    """Send JSON payload to all websockets belonging to username."""
    sent = False
    msg = json.dumps(payload)
    for client_ws, user in list(clients.items()):
        if user == username:
            try:
                await client_ws.send(msg)
                sent = True
            except Exception:
                pass
    return sent


async def send_file_to_user(username, payload):
    """Send file payload with rate limiting."""
    msg = json.dumps(payload).encode("utf-8")
    for client_ws, user in list(clients.items()):
        if user == username:
            try:
                await rate_limited_send(client_ws, msg)
                return True
            except Exception:
                pass
    return False


# ── WebSocket handler ─────────────────────────────────────────────────────────

async def handler(ws):
    username = None
    session_token = None
    client_ip = ws.remote_address[0] if ws.remote_address else ""
    debug(f"New connection from {client_ip}")

    try:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except Exception as e:
                debug(f"JSON parse error: {e}")
                continue

            t = data.get("type")

            # ── register ───────────────────────────────────────────────────
            if t == "register":
                email = data.get("email", "").strip()
                uname = data.get("username", "").strip()
                pwd = data.get("password", "")
                dev_id = data.get("device_id", "")
                dev_name = data.get("device_name", "Unknown device")

                if not email or not uname or not pwd:
                    await ws.send(json.dumps({"type": "error", "msg": "Please fill in all fields"}))
                    continue

                ok, err = create_user(email, uname, pwd, dev_id, dev_name, client_ip)
                if not ok:
                    await ws.send(json.dumps({"type": "error", "msg": err}))
                    continue

                asyncio.ensure_future(asyncio.to_thread(
                    send_email, email, "Welcome to BunsheeChat!",
                    f"Thanks for joining BunsheeChat!\n\nUsername: {uname}"
                ))
                await ws.send(json.dumps({"type": "register_success"}))
                continue

            # ── login ──────────────────────────────────────────────────────
            if t == "login":
                identifier = data.get("identifier", "").strip()
                pwd = data.get("password", "")
                dev_id = data.get("device_id", "")
                dev_name = data.get("device_name", "Unknown device")
                keep = data.get("keep_logged_in", False)

                if not identifier or not pwd:
                    await ws.send(json.dumps({"type": "error", "msg": "Please fill in all fields"}))
                    continue

                user = get_user_by_identifier(identifier)
                if not user:
                    await ws.send(json.dumps({"type": "error", "msg": "User does not exist"}))
                    continue

                if not verify_password(pwd, user["password"]):
                    await ws.send(json.dumps({"type": "error", "msg": "Wrong password"}))
                    continue

                stored_dev = user["last_device_id"] or ""
                if stored_dev and dev_id and stored_dev != dev_id:
                    loc = get_location_from_ip(client_ip)
                    body = (f"New device login detected!\n\nLocation: {loc}\n"
                            f"Device: {dev_name}\nIP: {client_ip}")
                    asyncio.ensure_future(asyncio.to_thread(
                        send_email, user["email"], "New device login on BunsheeChat", body
                    ))

                username = user["username"]
                clients[ws] = username
                update_login_meta(username, dev_id, dev_name, client_ip)

                new_token = None
                if keep:
                    new_token = create_session(username, dev_id, dev_name)
                    session_token = new_token

                profile = get_user_profile(username)
                blocked = get_blocked_users(username)
                await ws.send(json.dumps({
                    "type": "login_success",
                    "username": username,
                    "display_name": profile["display_name"] if profile else None,
                    "profile_picture": profile["profile_picture"] if profile else None,
                    "banner": profile["banner"] if profile else None,
                    "bio": profile["bio"] if profile else None,
                    "created_at": profile["created_at"] if profile else None,
                    "session_token": new_token,
                    "blocked_users": blocked
                }))

                chats = get_chats(username)
                await ws.send(json.dumps({"type": "chat_list", "chats": chats}))
                continue

            # ── token login ────────────────────────────────────────────────
            if t == "token_login":
                token = data.get("token", "")
                dev_id = data.get("device_id", "")
                dev_name = data.get("device_name", "Unknown device")

                session = validate_session(token)
                if not session:
                    await ws.send(json.dumps({"type": "token_invalid"}))
                    continue

                user = get_user_by_identifier(session["username"])
                if not user:
                    await ws.send(json.dumps({"type": "token_invalid"}))
                    continue

                username = user["username"]
                session_token = token
                clients[ws] = username
                update_login_meta(username, dev_id, dev_name, client_ip)

                profile = get_user_profile(username)
                blocked = get_blocked_users(username)
                await ws.send(json.dumps({
                    "type": "login_success",
                    "username": username,
                    "display_name": profile["display_name"] if profile else None,
                    "profile_picture": profile["profile_picture"] if profile else None,
                    "banner": profile["banner"] if profile else None,
                    "bio": profile["bio"] if profile else None,
                    "created_at": profile["created_at"] if profile else None,
                    "session_token": token,
                    "blocked_users": blocked
                }))
                chats = get_chats(username)
                await ws.send(json.dumps({"type": "chat_list", "chats": chats}))
                continue

            # ── logout ─────────────────────────────────────────────────────
            if t == "logout":
                tok = data.get("token", "") or session_token
                delete_session(tok)
                session_token = None
                username = None
                if ws in clients:
                    del clients[ws]
                await ws.send(json.dumps({"type": "logout_success"}))
                continue

            # ── Require authentication for everything below ─────────────────
            if not username:
                await ws.send(json.dumps({"type": "error", "msg": "Not authenticated"}))
                continue

            if t == "load_chats":
                chats = get_chats(username)
                await ws.send(json.dumps({"type": "chat_list", "chats": chats}))

            elif t == "search":
                query = data.get("query", "").strip()
                if query:
                    results = get_users_matching(query, username)
                    await ws.send(json.dumps({"type": "search_results", "results": results}))

            elif t == "get_chat":
                target = data.get("user", "")
                offset = data.get("offset", 0)
                limit = data.get("limit", 50)
                if target:
                    history = get_chat_history(username, target, limit=limit, offset=offset)
                    await ws.send(json.dumps({
                        "type": "chat_history",
                        "user": target,
                        "messages": history,
                        "offset": offset
                    }))

            elif t == "message":
                target = data.get("to", "")
                msg = data.get("msg", "")
                if not target or not msg:
                    continue
                if is_blocked(target, username):
                    continue
                msg_id, ts = save_message(username, target, msg)
                save_conversation(username, target, msg)

                payload = {
                    "type": "message",
                    "id": msg_id,
                    "from": username,
                    "msg": msg,
                    "timestamp": ts,
                    "msg_type": "text"
                }
                await send_to_user(target, payload)

            # ── File transfer ──────────────────────────────────────────────
            elif t == "file_start":
                # Client wants to start a file upload
                target = data.get("to", "")
                file_name = data.get("file_name", "file")
                file_size = data.get("file_size", 0)
                file_mime = data.get("file_mime", "application/octet-stream")
                total_chunks = data.get("total_chunks", 1)
                transfer_id = data.get("transfer_id", secrets.token_hex(8))

                if file_size > MAX_FILE_SIZE:
                    await ws.send(json.dumps({
                        "type": "file_error",
                        "transfer_id": transfer_id,
                        "msg": "File too large (max 2GB)"
                    }))
                    continue

                if is_blocked(target, username):
                    await ws.send(json.dumps({
                        "type": "file_error",
                        "transfer_id": transfer_id,
                        "msg": "Cannot send to this user"
                    }))
                    continue

                pending_transfers[transfer_id] = {
                    "sender": username,
                    "target": target,
                    "file_name": file_name,
                    "file_size": file_size,
                    "file_mime": file_mime,
                    "total_chunks": total_chunks,
                    "chunks": {},
                }
                await ws.send(json.dumps({
                    "type": "file_ready",
                    "transfer_id": transfer_id
                }))

            elif t == "file_chunk":
                transfer_id = data.get("transfer_id", "")
                chunk_idx = data.get("chunk_index", 0)
                chunk_data = data.get("data", "")

                if transfer_id not in pending_transfers:
                    await ws.send(json.dumps({
                        "type": "file_error",
                        "transfer_id": transfer_id,
                        "msg": "Unknown transfer"
                    }))
                    continue

                tf = pending_transfers[transfer_id]
                if tf["sender"] != username:
                    continue

                tf["chunks"][chunk_idx] = chunk_data
                received = len(tf["chunks"])
                total = tf["total_chunks"]

                # Ack the chunk
                await ws.send(json.dumps({
                    "type": "file_chunk_ack",
                    "transfer_id": transfer_id,
                    "chunk_index": chunk_idx,
                    "received": received,
                    "total": total
                }))

                # If all chunks received, reassemble and save
                if received == total:
                    chunks_ordered = [tf["chunks"][i] for i in range(total)]
                    full_data = "".join(chunks_ordered)

                    msg_id, ts = save_message(
                        username, tf["target"],
                        f"[File: {tf['file_name']}]",
                        msg_type=tf["file_mime"].split("/")[0] if "/" in tf["file_mime"] else "file",
                        file_name=tf["file_name"],
                        file_size=tf["file_size"],
                        file_mime=tf["file_mime"],
                        file_data=full_data
                    )
                    save_conversation(username, tf["target"], f"[File: {tf['file_name']}]")

                    payload = {
                        "type": "file_message",
                        "id": msg_id,
                        "from": username,
                        "msg": f"[File: {tf['file_name']}]",
                        "timestamp": ts,
                        "msg_type": tf["file_mime"].split("/")[0] if "/" in tf["file_mime"] else "file",
                        "file_name": tf["file_name"],
                        "file_size": tf["file_size"],
                        "file_mime": tf["file_mime"],
                        "file_data": full_data
                    }

                    # Send to recipient
                    asyncio.ensure_future(send_file_to_user(tf["target"], payload))

                    # Send sender a confirmation
                    await ws.send(json.dumps({
                        "type": "file_complete",
                        "transfer_id": transfer_id,
                        "id": msg_id,
                        "timestamp": ts,
                        "file_name": tf["file_name"],
                        "file_size": tf["file_size"],
                        "file_mime": tf["file_mime"],
                        "file_data": full_data
                    }))

                    del pending_transfers[transfer_id]

            # ── Profile updates ────────────────────────────────────────────
            elif t == "update_display_name":
                val = data.get("display_name", "").strip()
                if val:
                    update_field(username, "display_name", val)
                    await ws.send(json.dumps({"type": "profile_updated", "display_name": val}))

            elif t == "update_profile_picture":
                val = data.get("profile_picture", "")
                update_field(username, "profile_picture", val)
                await ws.send(json.dumps({"type": "profile_updated", "profile_picture": val}))

            elif t == "update_banner":
                val = data.get("banner", "")
                update_field(username, "banner", val)
                await ws.send(json.dumps({"type": "profile_updated", "banner": val}))

            elif t == "update_bio":
                val = data.get("bio", "")
                update_field(username, "bio", val)
                await ws.send(json.dumps({"type": "profile_updated", "bio": val}))

            elif t == "get_profile":
                target = data.get("user", "")
                if target:
                    p = get_user_profile(target)
                    await ws.send(json.dumps({
                        "type": "profile_data",
                        "username": p["username"] if p else target,
                        "display_name": p["display_name"] if p else None,
                        "profile_picture": p["profile_picture"] if p else None,
                        "banner": p["banner"] if p else None,
                        "bio": p["bio"] if p else None,
                        "created_at": p["created_at"] if p else None,
                    }))

            elif t == "block_user":
                target = data.get("user", "")
                if target:
                    success = block_user(username, target)
                    if success:
                        await ws.send(json.dumps({"type": "block_success", "user": target}))

            elif t == "unblock_user":
                target = data.get("user", "")
                if target:
                    success = unblock_user(username, target)
                    if success:
                        await ws.send(json.dumps({"type": "unblock_success", "user": target}))

    except websockets.exceptions.ConnectionClosed:
        debug(f"Connection closed: {username or client_ip}")
    except Exception as e:
        debug(f"Handler exception for {username or client_ip}: {e}")
    finally:
        if ws in clients:
            debug(f"User disconnected: {clients[ws]}")
            del clients[ws]
        if ws in bandwidth_tracker:
            del bandwidth_tracker[ws]


async def main():
    init_db()
    debug("Starting BunsheeChat server...")

    async with websockets.serve(
        handler,
        "0.0.0.0",
        3333,
        max_size=None,          # No message size limit (we handle large files)
        ping_interval=30,
        ping_timeout=60,
        close_timeout=10,
    ):
        debug("Server running on port 3333")
        await asyncio.Future()


asyncio.run(main())