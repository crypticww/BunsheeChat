import asyncio
import base64
import io
import json
import mimetypes
import os
import platform
import secrets
import subprocess
import threading
import time
import uuid
from pathlib import Path
from tkinter import filedialog
import tempfile
from typing import Optional, List, Tuple, Any

import customtkinter as ctk

try:
    from PIL import Image, ImageDraw, ImageSequence
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pillow"])
    from PIL import Image, ImageDraw, ImageSequence

try:
    import websockets
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
    import websockets

HOST        = "wss://musty-uneaten-electable.ngrok-free.dev"
DEVICE_FILE = Path.home() / ".bunsheechat_device_id"
SESSION_FILE= Path.home() / ".bunsheechat_session"

CHUNK_SIZE  = 512 * 1024
MAX_FILE_SIZE = 2 * 1024 * 1024 * 1024

# Modern color scheme - cleaner, more professional
BG_DARKEST   = "#0a0e14"
BG_DARK      = "#0f1419"
BG_PANEL     = "#151a21"
BG_CARD      = "#1a1f28"
BG_HOVER     = "#1e2530"
BG_INPUT     = "#232930"
ACCENT       = "#3b82f6"
ACCENT_DARK  = "#2563eb"
ACCENT_GLOW  = "#60a5fa"
SUCCESS      = "#10b981"
DANGER       = "#ef4444"
DANGER_HOVER = "#dc2626"
WARNING      = "#f59e0b"
TEXT_PRI     = "#f3f4f6"
TEXT_SEC     = "#9ca3af"
TEXT_MUTED   = "#6b7280"
BORDER       = "#374151"
ONLINE       = "#10b981"
MSG_BG_ME    = "#1e3a5f"
MSG_BG_OTHER = "#1e2530"

FONT_FAMILY = "Segoe UI" if platform.system() == "Windows" else "SF Pro Display" if platform.system() == "Darwin" else "Ubuntu"

def human_size(n: Optional[int]) -> str:
    if n is None:
        return "?"
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

def fmt_time(ts: Optional[str]) -> str:
    from datetime import datetime
    if not ts:
        return datetime.now().strftime("%H:%M")
    try:
        dt = datetime.fromisoformat(ts)
        return dt.strftime("%H:%M")
    except Exception:
        return str(ts)

def make_circle_image(img: Image.Image, size: int) -> Image.Image:
    img = img.convert("RGBA")
    w, h = img.size
    m = min(w, h)
    img = img.crop(((w - m) // 2, (h - m) // 2, (w + m) // 2, (h + m) // 2))
    img = img.resize((size, size), Image.Resampling.LANCZOS)
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
    out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    out.paste(img, (0, 0), mask)
    return out

def make_circle_image_from_b64(b64_str: Optional[str], size: int) -> Optional[Image.Image]:
    if not b64_str:
        return None
    try:
        data = base64.b64decode(b64_str)
        img = Image.open(io.BytesIO(data))
        return make_circle_image(img, size)
    except Exception:
        return None

def load_gif_frames(b64_str: Optional[str], size: Optional[int] = None) -> Optional[List[Tuple[Image.Image, int]]]:
    if not b64_str:
        return None
    try:
        data = base64.b64decode(b64_str)
        img = Image.open(io.BytesIO(data))
        if not hasattr(img, 'n_frames') or img.n_frames <= 1:
            return None
        frames = []
        for frame in ImageSequence.Iterator(img):
            dur = frame.info.get("duration", 100)
            f = frame.copy().convert("RGBA")
            if size:
                f = f.resize((size, size), Image.Resampling.LANCZOS)
            frames.append((f, dur))
        return frames
    except Exception:
        return None

class WsClient:
    def __init__(self, app_instance):
        self.app = app_instance
        self.ws = None
        self.loop = None
        self.connected = False

    def get_device_id(self) -> str:
        if DEVICE_FILE.exists():
            return DEVICE_FILE.read_text().strip()
        did = uuid.uuid4().hex
        DEVICE_FILE.write_text(did)
        return did

    def get_device_name(self) -> str:
        return f"{platform.system()} {platform.release()} ({platform.node()})"

    async def _connect(self):
        while True:
            try:
                self.ws = await websockets.connect(
                    HOST,
                    max_size=None,
                    ping_interval=30,
                    ping_timeout=60,
                )
                self.connected = True
                self.app.after(0, self.app.on_connected)
                async for raw in self.ws:
                    try:
                        data = json.loads(raw)
                        self.app.after(0, lambda d=data: self.app.handle_server(d))
                    except Exception:
                        pass
            except Exception:
                pass
            self.connected = False
            self.app.after(0, self.app.on_disconnected)
            await asyncio.sleep(3)

    def start(self):
        def run():
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)
            self.loop.run_until_complete(self._connect())
        threading.Thread(target=run, daemon=True).start()

    def send(self, data: dict):
        if not self.ws or not self.loop:
            return
        asyncio.run_coroutine_threadsafe(
            self.ws.send(json.dumps(data)), self.loop
        )

    def send_raw(self, text: str):
        if not self.ws or not self.loop:
            return
        asyncio.run_coroutine_threadsafe(self.ws.send(text), self.loop)

class AnimatedLabel(ctk.CTkLabel):
    def __init__(self, master, frames: List[Tuple[Image.Image, int]], **kwargs):
        super().__init__(master, text="", **kwargs)
        self._frames = frames
        self._idx = 0
        self._job = None
        self._play()

    def _play(self):
        if not self._frames:
            return
        img_pil, dur = self._frames[self._idx]
        ctk_img = ctk.CTkImage(img_pil, size=img_pil.size)
        self.configure(image=ctk_img)
        self._idx = (self._idx + 1) % len(self._frames)
        self._job = self.after(max(dur, 20), self._play)

    def stop(self):
        if self._job:
            self.after_cancel(self._job)

class GradientFrame(ctk.CTkFrame):
    def __init__(self, master, color1: str, color2: str, **kwargs):
        kwargs.setdefault("fg_color", "transparent")
        super().__init__(master, **kwargs)
        self.color1 = color1
        self.color2 = color2
        import tkinter as tk
        self._canvas = tk.Canvas(self, highlightthickness=0, bd=0, bg=color1)
        self._canvas.place(relwidth=1, relheight=1)
        self.bind("<Configure>", self._draw)

    def _draw(self, _event=None, **_kwargs):  # pylint: disable=unused-argument
        w = self.winfo_width()
        h = self.winfo_height()
        if w <= 1 or h <= 1:
            return
        self._canvas.delete("gradient")
        r1, g1, b1 = self._hex_to_rgb(self.color1)
        r2, g2, b2 = self._hex_to_rgb(self.color2)
        steps = min(h, 256)
        for i in range(steps):
            t = i / steps
            r = int(r1 + (r2 - r1) * t)
            g = int(g1 + (g2 - g1) * t)
            b = int(b1 + (b2 - b1) * t)
            y0 = int(i * h / steps)
            y1 = int((i + 1) * h / steps) + 1
            color = f"#{r:02x}{g:02x}{b:02x}"
            self._canvas.create_rectangle(0, y0, w, y1, fill=color, outline="", tags="gradient")

    @staticmethod
    def _hex_to_rgb(h: str) -> Tuple[int, int, int]:
        h = h.lstrip("#")
        return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)

class AvatarWidget(ctk.CTkLabel):
    def __init__(self, master, username: str = "?", b64_data: Optional[str] = None, size: int = 40, **kwargs):
        self._size = size
        self._username = username
        self._frames: Optional[List[Tuple[Image.Image, int]]] = None
        self._job = None
        self._frame_idx = 0

        if b64_data:
            frames = load_gif_frames(b64_data, size)
            if frames:
                first, _ = frames[0]
                mask = Image.new("L", (size, size), 0)
                ImageDraw.Draw(mask).ellipse((0, 0, size, size), fill=255)
                out = Image.new("RGBA", (size, size), (0, 0, 0, 0))
                first_sq = first.resize((size, size))
                out.paste(first_sq, (0, 0), mask)
                ctk_img = ctk.CTkImage(out, size=(size, size))
                super().__init__(master, image=ctk_img, text="", width=size, height=size, **kwargs)
                self._frames = frames
                return

            pil_img = make_circle_image_from_b64(b64_data, size)
            if pil_img:
                ctk_img = ctk.CTkImage(pil_img, size=(size, size))
                super().__init__(master, image=ctk_img, text="", width=size, height=size, **kwargs)
                return

        initial = (username[0].upper()) if username else "?"
        color = ACCENT
        bg = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(bg)
        draw.ellipse((0, 0, size - 1, size - 1), fill=self._hex_to_rgb(color) + (255,))
        ctk_img = ctk.CTkImage(bg, size=(size, size))
        super().__init__(master, image=ctk_img, text=initial, compound="center",
                         width=size, height=size,
                         font=ctk.CTkFont(family=FONT_FAMILY, size=max(size // 3, 10), weight="bold"),
                         text_color="#ffffff", **kwargs)

    @staticmethod
    def _hex_to_rgb(h: str) -> Tuple[int, int, int]:
        h = h.lstrip("#")
        return int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)

    def start_animation(self):
        if self._frames:
            self._animate()

    def _animate(self):
        if not self._frames:
            return
        img_pil, dur = self._frames[self._frame_idx]
        mask = Image.new("L", (self._size, self._size), 0)
        ImageDraw.Draw(mask).ellipse((0, 0, self._size, self._size), fill=255)
        out = Image.new("RGBA", (self._size, self._size), (0, 0, 0, 0))
        out.paste(img_pil.resize((self._size, self._size)), (0, 0), mask)
        ctk_img = ctk.CTkImage(out, size=(self._size, self._size))
        self.configure(image=ctk_img, text="")
        self._frame_idx = (self._frame_idx + 1) % len(self._frames)
        self._job = self.after(max(dur, 20), self._animate)

    def stop_animation(self):
        if self._job:
            self.after_cancel(self._job)
            self._job = None

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        ctk.set_appearance_mode("dark")
        self.configure(fg_color=BG_DARK)
        self.title("BunsheeChat")
        self.geometry("1100x700")
        self.minsize(900, 600)

        self.client = WsClient(self)
        self.username: Optional[str] = None
        self.display_name: Optional[str] = None
        self.profile_picture: Optional[str] = None
        self.banner: Optional[str] = None
        self.bio: Optional[str] = None
        self.created_at: Optional[str] = None
        self.session_token: Optional[str] = None
        self.blocked_users: set = set()
        self.current_chat: Optional[str] = None
        self.chat_buttons: dict = {}
        self._pending_transfers: dict = {}
        self._active_panel: Optional[str] = None
        self._avatar_animations: list = []
        self.pending_attachment: Optional[dict] = None
        self._attachment_frame: Optional[ctk.CTkFrame] = None
        self._saved_chat_widgets: Optional[dict] = None
        self._profile_view_frame: Optional[ctk.CTkFrame] = None

        self.client.start()
        self._build_auth("login")

    def on_connected(self):
        if hasattr(self, "_conn_dot"):
            self._conn_dot.configure(text="● Connected", text_color=SUCCESS)
        if self.username is None:
            tok = self._load_token()
            if tok:
                self.client.send({
                    "type": "token_login", "token": tok,
                    "device_id": self.client.get_device_id(),
                    "device_name": self.client.get_device_name()
                })

    def on_disconnected(self):
        if hasattr(self, "_conn_dot"):
            self._conn_dot.configure(text="● Disconnected", text_color=DANGER)

    def _save_token(self, t: str):
        if t: 
            SESSION_FILE.write_text(t)

    def _load_token(self) -> Optional[str]:
        return SESSION_FILE.read_text().strip() if SESSION_FILE.exists() else None

    def _clear_token(self):
        if SESSION_FILE.exists(): 
            SESSION_FILE.unlink()

    def clear(self):
        for w in self.winfo_children():
            w.destroy()
        self._avatar_animations.clear()

    def _entry(self, parent, placeholder: str, show: Optional[str] = None, width: int = 300):
        kw = dict(
            placeholder_text=placeholder, width=width, height=44,
            corner_radius=10, fg_color=BG_INPUT, border_color=BORDER,
            border_width=1, text_color=TEXT_PRI,
            placeholder_text_color=TEXT_SEC,
            font=ctk.CTkFont(family=FONT_FAMILY, size=13)
        )
        if show: 
            kw["show"] = show
        e = ctk.CTkEntry(parent, **kw)
        e.pack(pady=6, fill="x")
        return e

    def _btn(self, parent, text: str, cmd, fg: str = ACCENT, hover: str = ACCENT_DARK, height: int = 44):
        b = ctk.CTkButton(
            parent, text=text, height=height, corner_radius=10,
            fg_color=fg, hover_color=hover,
            font=ctk.CTkFont(family=FONT_FAMILY, size=13, weight="bold"),
            text_color="#ffffff", command=cmd
        )
        b.pack(fill="x", pady=(10, 0))
        return b

    def _label(self, parent, text: str, size: int = 12, color: str = TEXT_SEC, bold: bool = False, anchor: str = "w"):
        return ctk.CTkLabel(
            parent, text=text,
            font=ctk.CTkFont(family=FONT_FAMILY, size=size,
                             weight="bold" if bold else "normal"),
            text_color=color, anchor=anchor
        )

    def _divider(self, parent, pady: int = 10):
        ctk.CTkFrame(parent, height=1, fg_color=BORDER).pack(fill="x", pady=pady)

    def _build_auth(self, mode: str = "login"):
        self.clear()
        self._auth_mode = mode

        grad = GradientFrame(self, BG_DARKEST, BG_DARK)
        grad.place(relwidth=1, relheight=1)

        outer = ctk.CTkFrame(self, fg_color="transparent")
        outer.place(relx=0.5, rely=0.5, anchor="center")

        logo_frame = ctk.CTkFrame(outer, fg_color="transparent")
        logo_frame.pack(pady=(0, 8))

        self._label(logo_frame, "BunsheeChat", size=32, color=TEXT_PRI, bold=True, anchor="center").pack()
        self._conn_dot = self._label(logo_frame, "● Connecting…", size=11, color=TEXT_SEC, anchor="center")
        self._conn_dot.pack(pady=(4, 0))

        card = ctk.CTkFrame(outer, fg_color=BG_CARD, corner_radius=18, width=400)
        card.pack(pady=12)

        inner = ctk.CTkFrame(card, fg_color="transparent")
        inner.pack(padx=36, pady=32, fill="both", expand=True)

        tab_row = ctk.CTkFrame(inner, fg_color=BG_INPUT, corner_radius=10, height=46)
        tab_row.pack(fill="x", pady=(0, 20))
        tab_row.pack_propagate(False)

        def _tab(label: str, m: str):
            active = (mode == m)
            ctk.CTkButton(
                tab_row, text=label, width=140, height=38, corner_radius=8,
                fg_color=ACCENT if active else "transparent",
                hover_color=ACCENT_DARK if active else BG_HOVER,
                text_color=TEXT_PRI if active else TEXT_SEC,
                font=ctk.CTkFont(family=FONT_FAMILY, size=13,
                                 weight="bold" if active else "normal"),
                command=lambda: self._build_auth(m)
            ).pack(side="left", padx=4, pady=4, expand=True, fill="both")

        _tab("Login", "login")
        _tab("Register", "register")

        self._status = ctk.CTkLabel(
            inner, text="",
            font=ctk.CTkFont(family=FONT_FAMILY, size=11),
            text_color=DANGER, wraplength=330
        )
        self._status.pack(fill="x", pady=(0, 6))

        if mode == "login":
            self._id_entry = self._entry(inner, "Email or username")
            self._pw_entry = self._entry(inner, "Password", show="*")

            self._keep_var = ctk.BooleanVar(value=False)
            row = ctk.CTkFrame(inner, fg_color="transparent")
            row.pack(fill="x", pady=(8, 0))
            ctk.CTkCheckBox(
                row, text="Keep me logged in",
                variable=self._keep_var,
                font=ctk.CTkFont(family=FONT_FAMILY, size=12),
                text_color=TEXT_SEC, fg_color=ACCENT,
                hover_color=ACCENT_DARK, border_color=BORDER,
                checkmark_color="#fff", width=22, height=22
            ).pack(side="left")

            self._btn(inner, "Login", self._submit_auth)
        else:
            self._email_entry = self._entry(inner, "Email")
            self._uname_entry = self._entry(inner, "Username")
            self._pw_entry = self._entry(inner, "Password", show="*")
            self._btn(inner, "Create Account", self._submit_auth)

    def _submit_auth(self):
        payload = {
            "device_id": self.client.get_device_id(),
            "device_name": self.client.get_device_name()
        }
        if self._auth_mode == "login":
            payload.update({
                "type": "login",
                "identifier": self._id_entry.get().strip(),
                "password": self._pw_entry.get(),
                "keep_logged_in": self._keep_var.get()
            })
        else:
            payload.update({
                "type": "register",
                "email": self._email_entry.get().strip(),
                "username": self._uname_entry.get().strip(),
                "password": self._pw_entry.get()
            })
        self._status.configure(text="")
        self._send_when_ready(payload)

    def _send_when_ready(self, payload: dict, retries: int = 25):
        if self.client.connected and self.client.ws:
            self.client.send(payload)
        elif retries > 0:
            self.after(200, lambda: self._send_when_ready(payload, retries - 1))
        else:
            if hasattr(self, "_status"):
                self._status.configure(text="Server not reachable")

    def _build_chat_ui(self):
        self.clear()

        self._left = ctk.CTkFrame(self, fg_color=BG_PANEL, width=280, corner_radius=0)
        self._left.pack(side="left", fill="y")
        self._left.pack_propagate(False)

        self._right = ctk.CTkFrame(self, fg_color=BG_DARK, corner_radius=0)
        self._right.pack(side="right", fill="both", expand=True)

        self._build_sidebar()
        self._build_chat_area()
        self.client.send({"type": "load_chats"})

    def _build_sidebar(self):
        left = self._left

        hdr = ctk.CTkFrame(left, fg_color=BG_CARD, height=60, corner_radius=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        self._conn_dot = ctk.CTkLabel(
            hdr, text="●", text_color=SUCCESS,
            font=ctk.CTkFont(size=10)
        )
        self._conn_dot.place(x=16, rely=0.5, anchor="w")

        ctk.CTkLabel(
            hdr, text="BunsheeChat",
            font=ctk.CTkFont(family=FONT_FAMILY, size=15, weight="bold"),
            text_color=TEXT_PRI
        ).place(relx=0.5, rely=0.5, anchor="center")

        ctk.CTkButton(
            hdr, text="⚙", width=36, height=36, corner_radius=8,
            fg_color="transparent", hover_color=BG_HOVER,
            text_color=TEXT_SEC, font=ctk.CTkFont(size=16),
            command=self._toggle_settings
        ).place(relx=1.0, x=-10, rely=0.5, anchor="e")

        search_frame = ctk.CTkFrame(left, fg_color="transparent")
        search_frame.pack(fill="x", padx=12, pady=(10, 6))

        self._search_entry = ctk.CTkEntry(
            search_frame, placeholder_text="🔍  Search chats",
            height=38, corner_radius=10,
            fg_color=BG_INPUT, border_color=BORDER, border_width=1,
            text_color=TEXT_PRI, placeholder_text_color=TEXT_SEC,
            font=ctk.CTkFont(family=FONT_FAMILY, size=12)
        )
        self._search_entry.pack(fill="x")
        self._search_entry.bind("<KeyRelease>", self._on_search)

        self._divider(left, pady=6)

        self._chat_list = ctk.CTkScrollableFrame(
            left, fg_color="transparent", corner_radius=0,
            scrollbar_button_color=BG_HOVER,
            scrollbar_button_hover_color=ACCENT
        )
        self._chat_list.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        footer = ctk.CTkFrame(left, fg_color=BG_CARD, height=64, corner_radius=0)
        footer.pack(fill="x", side="bottom")
        footer.pack_propagate(False)

        self._own_avatar = AvatarWidget(
            footer, username=self.username or "?",
            b64_data=self.profile_picture, size=40
        )
        self._own_avatar.place(x=12, rely=0.5, anchor="w")
        self._own_avatar.start_animation()

        name = self.display_name or self.username or ""
        self._footer_name = ctk.CTkLabel(
            footer, text=name,
            font=ctk.CTkFont(family=FONT_FAMILY, size=13, weight="bold"),
            text_color=ACCENT_GLOW, anchor="w"
        )
        self._footer_name.place(x=60, y=12)

        self._footer_user = ctk.CTkLabel(
            footer, text=f"@{self.username or ''}",
            font=ctk.CTkFont(family=FONT_FAMILY, size=11),
            text_color=TEXT_SEC, anchor="w"
        )
        self._footer_user.place(x=60, y=32)

    def _build_chat_area(self):
        right = self._right

        self._chat_hdr = ctk.CTkFrame(right, fg_color=BG_PANEL, height=60, corner_radius=0)
        self._chat_hdr.pack(fill="x", side="top")
        self._chat_hdr.pack_propagate(False)

        self._chat_hdr_name = ctk.CTkLabel(
            self._chat_hdr, text="",
            font=ctk.CTkFont(family=FONT_FAMILY, size=15, weight="bold"),
            text_color=ACCENT_GLOW
        )
        self._chat_hdr_name.pack(side="left", padx=20)

        self._more_btn = ctk.CTkButton(
            self._chat_hdr, text="⋮", width=38, height=38, corner_radius=8,
            fg_color="transparent", hover_color=BG_HOVER,
            text_color=TEXT_SEC, font=ctk.CTkFont(size=18),
            command=self._show_inline_menu
        )
        self._more_btn.pack(side="right", padx=10, pady=11)

        ctk.CTkButton(
            self._chat_hdr, text="👤", width=38, height=38, corner_radius=8,
            fg_color="transparent", hover_color=BG_HOVER,
            text_color=TEXT_SEC, font=ctk.CTkFont(size=16),
            command=lambda: self._show_user_profile(self.current_chat)
        ).pack(side="right", padx=4, pady=11)

        self._inline_menu = ctk.CTkFrame(
            right, fg_color=BG_CARD, corner_radius=12, width=180
        )

        self._msg_scroll = ctk.CTkScrollableFrame(
            right, fg_color=BG_DARK, corner_radius=0,
            scrollbar_button_color=BG_HOVER,
            scrollbar_button_hover_color=ACCENT
        )
        self._msg_scroll.pack(fill="both", expand=True)

        self._welcome = ctk.CTkLabel(
            self._msg_scroll,
            text="Select a conversation to start chatting",
            font=ctk.CTkFont(family=FONT_FAMILY, size=14),
            text_color=TEXT_SEC
        )
        self._welcome.pack(expand=True, pady=80)

        self._attachment_frame = ctk.CTkFrame(right, fg_color=BG_PANEL, height=80, corner_radius=0)

        self._input_bar = ctk.CTkFrame(right, fg_color=BG_PANEL, height=70, corner_radius=0)
        self._input_bar.pack(fill="x", side="bottom")
        self._input_bar.pack_propagate(False)

        ctk.CTkButton(
            self._input_bar, text="📎", width=42, height=44, corner_radius=10,
            fg_color=BG_INPUT, hover_color=BG_HOVER,
            text_color=TEXT_SEC, font=ctk.CTkFont(size=16),
            command=self._pick_file
        ).pack(side="left", padx=(12, 6), pady=13)

        self._msg_entry = ctk.CTkEntry(
            self._input_bar,
            placeholder_text="Type a message...",
            height=44, corner_radius=10,
            fg_color=BG_INPUT, border_color=BORDER, border_width=1,
            text_color=TEXT_PRI, placeholder_text_color=TEXT_SEC,
            font=ctk.CTkFont(family=FONT_FAMILY, size=13)
        )
        self._msg_entry.pack(side="left", fill="x", expand=True, padx=6, pady=13)
        self._msg_entry.bind("<Return>", lambda _e: self._send_msg())  # pylint: disable=unused-argument

        ctk.CTkButton(
            self._input_bar, text="Send", width=80, height=44, corner_radius=10,
            fg_color=ACCENT, hover_color=ACCENT_DARK,
            font=ctk.CTkFont(family=FONT_FAMILY, size=13, weight="bold"),
            text_color="#fff", command=self._send_msg
        ).pack(side="right", padx=(6, 12), pady=13)

    def _show_inline_menu(self):
        if not self.current_chat:
            return

        if self._inline_menu.winfo_ismapped():
            self._inline_menu.place_forget()
            return

        for w in self._inline_menu.winfo_children():
            w.destroy()

        is_blocked = self.current_chat in self.blocked_users
        label = "Unblock User" if is_blocked else "Block User"
        color = DANGER if not is_blocked else SUCCESS

        ctk.CTkButton(
            self._inline_menu, text=label, height=36, corner_radius=8,
            fg_color="transparent", hover_color=BG_HOVER,
            text_color=color,
            font=ctk.CTkFont(family=FONT_FAMILY, size=12, weight="bold"),
            command=self._toggle_block_inline
        ).pack(fill="x", padx=8, pady=(8, 4))

        ctk.CTkButton(
            self._inline_menu, text="View Profile", height=36, corner_radius=8,
            fg_color="transparent", hover_color=BG_HOVER,
            text_color=TEXT_PRI,
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            command=lambda: (self._inline_menu.place_forget(),
                             self._show_user_profile(self.current_chat))
        ).pack(fill="x", padx=8, pady=(0, 8))

        self._inline_menu.place(
            in_=self._right, relx=1.0, x=-16, y=70, anchor="ne"
        )
        self._inline_menu.lift()

    def _toggle_block_inline(self):
        self._inline_menu.place_forget()
        if not self.current_chat:
            return
        if self.current_chat in self.blocked_users:
            self.client.send({"type": "unblock_user", "user": self.current_chat})
        else:
            self.client.send({"type": "block_user", "user": self.current_chat})

    def _toggle_settings(self):
        if self._active_panel == "settings":
            self._close_panel()
        else:
            self._open_settings_panel()

    def _open_settings_panel(self):
        self._close_panel()
        self._active_panel = "settings"

        panel = ctk.CTkFrame(self._right, fg_color=BG_CARD, corner_radius=0, width=340)
        panel.pack(side="right", fill="both", expand=True)
        panel.pack_propagate(False)
        self._panel_widget = panel

        self._build_settings_in(panel)

    def _open_profile_panel(self):
        self._close_panel()
        self._active_panel = "profile"

        panel = ctk.CTkFrame(self._right, fg_color=BG_CARD, corner_radius=0, width=360)
        panel.pack(side="right", fill="both", expand=True)
        panel.pack_propagate(False)
        self._panel_widget = panel

        self._build_profile_in(panel)

    def _close_panel(self):
        if hasattr(self, "_panel_widget") and self._panel_widget.winfo_exists():
            self._panel_widget.pack_forget()
            self._panel_widget.destroy()
        self._active_panel = None

    def _panel_header(self, parent, title: str):
        hdr = ctk.CTkFrame(parent, fg_color=BG_PANEL, height=60, corner_radius=0)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)
        ctk.CTkLabel(
            hdr, text=title,
            font=ctk.CTkFont(family=FONT_FAMILY, size=15, weight="bold"),
            text_color=TEXT_PRI
        ).place(x=18, rely=0.5, anchor="w")
        ctk.CTkButton(
            hdr, text="✕", width=36, height=36, corner_radius=8,
            fg_color="transparent", hover_color=BG_HOVER,
            text_color=TEXT_SEC, font=ctk.CTkFont(size=14),
            command=self._close_panel
        ).place(relx=1.0, x=-10, rely=0.5, anchor="e")

    def _build_settings_in(self, parent):
        self._panel_header(parent, "Settings")

        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent",
                                        scrollbar_button_color=BG_HOVER)
        scroll.pack(fill="both", expand=True, padx=14, pady=10)

        self._section_label(scroll, "ACCOUNT")
        self._info_card(scroll, "Logged in as", self.username or "")
        self._divider(scroll)

        self._btn_card(scroll, "Edit Profile", "Customize your avatar, bio & more",
                       cmd=self._open_profile_panel)
        self._divider(scroll)

        self._btn_card(scroll, "Log Out", "Sign out and return to login screen",
                       btn_text="Log Out", btn_fg=DANGER, btn_hover=DANGER_HOVER,
                       cmd=self._logout)
        self._divider(scroll)

        self._section_label(scroll, "ABOUT")
        self._info_card(scroll, "App", "BunsheeChat v2.0")
        self._info_card(scroll, "Device", self.client.get_device_name())

    def _build_profile_in(self, parent):
        self._panel_header(parent, "Edit Profile")

        scroll = ctk.CTkScrollableFrame(parent, fg_color="transparent",
                                        scrollbar_button_color=BG_HOVER)
        scroll.pack(fill="both", expand=True, padx=14, pady=10)

        self._section_label(scroll, "AVATAR")
        av_frame = ctk.CTkFrame(scroll, fg_color=BG_INPUT, corner_radius=12)
        av_frame.pack(fill="x", pady=6)

        self._profile_av = AvatarWidget(
            av_frame, username=self.username or "?",
            b64_data=self.profile_picture, size=80
        )
        self._profile_av.pack(pady=14)
        self._profile_av.start_animation()

        ctk.CTkButton(
            av_frame, text="Upload Avatar (PNG/JPG/GIF)",
            height=36, corner_radius=8,
            fg_color=ACCENT, hover_color=ACCENT_DARK,
            font=ctk.CTkFont(family=FONT_FAMILY, size=11),
            text_color="#fff",
            command=self._upload_avatar
        ).pack(fill="x", padx=12, pady=(0, 12))

        self._divider(scroll)

        self._section_label(scroll, "BANNER")
        self._banner_frame = ctk.CTkFrame(scroll, fg_color=BG_INPUT, corner_radius=12, height=90)
        self._banner_frame.pack(fill="x", pady=6)
        self._banner_frame.pack_propagate(False)
        self._refresh_banner_preview()

        ctk.CTkButton(
            scroll, text="Upload Banner (PNG/JPG/GIF)",
            height=36, corner_radius=8,
            fg_color=BG_INPUT, hover_color=BG_HOVER,
            font=ctk.CTkFont(family=FONT_FAMILY, size=11),
            text_color=TEXT_PRI,
            command=self._upload_banner
        ).pack(fill="x", pady=(6, 0))

        self._divider(scroll)

        self._section_label(scroll, "DISPLAY NAME")
        self._dn_entry = ctk.CTkEntry(
            scroll, placeholder_text="Display name",
            height=40, corner_radius=8,
            fg_color=BG_INPUT, border_color=BORDER, border_width=1,
            text_color=TEXT_PRI, placeholder_text_color=TEXT_SEC
        )
        if self.display_name:
            self._dn_entry.insert(0, self.display_name)
        self._dn_entry.pack(fill="x", pady=6)
        ctk.CTkButton(
            scroll, text="Save Name", height=36, corner_radius=8,
            fg_color=ACCENT, hover_color=ACCENT_DARK, text_color="#fff",
            command=self._save_display_name
        ).pack(fill="x", pady=(6, 0))

        self._divider(scroll)

        self._section_label(scroll, "BIO")
        self._bio_box = ctk.CTkTextbox(
            scroll, height=90, corner_radius=8,
            fg_color=BG_INPUT, border_color=BORDER, border_width=1,
            text_color=TEXT_PRI,
            font=ctk.CTkFont(family=FONT_FAMILY, size=12)
        )
        self._bio_box.pack(fill="x", pady=6)
        if self.bio:
            self._bio_box.insert("1.0", self.bio)

        self._save_bio_btn = ctk.CTkButton(
            scroll, text="Save Bio", height=36, corner_radius=8,
            fg_color=ACCENT, hover_color=ACCENT_DARK, text_color="#fff",
            command=self._save_bio
        )
        self._save_bio_btn.pack(fill="x", pady=(6, 0))

        self._divider(scroll)

        self._section_label(scroll, "ACCOUNT INFO")
        self._info_card(scroll, "Username", self.username or "")
        if self.created_at:
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(self.created_at)
                self._info_card(scroll, "Joined", dt.strftime("%d %b %Y"))
            except Exception:
                pass

    def _section_label(self, parent, text: str):
        ctk.CTkLabel(
            parent, text=text,
            font=ctk.CTkFont(family=FONT_FAMILY, size=10, weight="bold"),
            text_color=TEXT_SEC, anchor="w"
        ).pack(fill="x", padx=6, pady=(14, 6))

    def _info_card(self, parent, key: str, value: str):
        row = ctk.CTkFrame(parent, fg_color=BG_HOVER, corner_radius=8)
        row.pack(fill="x", pady=3)
        ctk.CTkLabel(row, text=key, font=ctk.CTkFont(family=FONT_FAMILY, size=12),
                     text_color=TEXT_SEC, anchor="w").pack(side="left", padx=14, pady=10)
        ctk.CTkLabel(row, text=str(value),
                     font=ctk.CTkFont(family=FONT_FAMILY, size=12, weight="bold"),
                     text_color=TEXT_PRI, anchor="e").pack(side="right", padx=14, pady=10)

    def _btn_card(self, parent, label: str, desc: str, btn_text: Optional[str] = None, 
                  btn_fg: str = ACCENT, btn_hover: str = ACCENT_DARK, cmd=None):
        card = ctk.CTkFrame(parent, fg_color=BG_HOVER, corner_radius=8)
        card.pack(fill="x", pady=3)
        ctk.CTkLabel(card, text=label,
                     font=ctk.CTkFont(family=FONT_FAMILY, size=12, weight="bold"),
                     text_color=TEXT_PRI, anchor="w").pack(fill="x", padx=14, pady=(12, 3))
        ctk.CTkLabel(card, text=desc,
                     font=ctk.CTkFont(family=FONT_FAMILY, size=11),
                     text_color=TEXT_SEC, anchor="w", wraplength=290, justify="left"
                     ).pack(fill="x", padx=14)
        ctk.CTkButton(
            card, text=btn_text or label, height=34, corner_radius=8,
            fg_color=btn_fg, hover_color=btn_hover,
            font=ctk.CTkFont(family=FONT_FAMILY, size=11, weight="bold"),
            text_color="#fff", command=cmd
        ).pack(fill="x", padx=14, pady=(8, 12))

    def _refresh_banner_preview(self):
        for w in self._banner_frame.winfo_children():
            w.destroy()
        if self.banner:
            frames = load_gif_frames(self.banner)
            if frames:
                resized = []
                for f, d in frames:
                    rf = f.resize((320, 90), Image.Resampling.LANCZOS)
                    resized.append((rf, d))
                lbl = AnimatedLabel(self._banner_frame, resized)
                lbl.pack(expand=True)
                return
            try:
                data = base64.b64decode(self.banner)
                img = Image.open(io.BytesIO(data)).resize((320, 90), Image.Resampling.LANCZOS)
                ctk_img = ctk.CTkImage(img, size=(320, 90))
                ctk.CTkLabel(self._banner_frame, image=ctk_img, text="").pack(expand=True)
            except Exception:
                pass
        else:
            ctk.CTkLabel(self._banner_frame, text="No banner",
                         text_color=TEXT_SEC,
                         font=ctk.CTkFont(family=FONT_FAMILY, size=11)).pack(expand=True)

    def _upload_avatar(self):
        fp = filedialog.askopenfilename(
            filetypes=[("Images", "*.png *.jpg *.jpeg *.gif *.bmp *.webp")]
        )
        if not fp:
            return
        try:
            with open(fp, "rb") as f:
                raw = f.read()
            if fp.lower().endswith(".gif"):
                b64 = base64.b64encode(raw).decode()
            else:
                img = Image.open(io.BytesIO(raw)).convert("RGBA")
                img = img.resize((256, 256), Image.Resampling.LANCZOS)
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                b64 = base64.b64encode(buf.getvalue()).decode()

            self.profile_picture = b64
            self.client.send({"type": "update_profile_picture", "profile_picture": b64})

            if hasattr(self, "_profile_av") and self._profile_av.winfo_exists():
                self._profile_av.destroy()
                self._profile_av = AvatarWidget(
                    self._profile_av.master,
                    username=self.username or "?", b64_data=b64, size=80
                )
                self._profile_av.pack(pady=14)
                self._profile_av.start_animation()

            self._refresh_own_avatar()
        except Exception as err:
            print(f"Avatar upload error: {err}")

    def _upload_banner(self):
        fp = filedialog.askopenfilename(
            filetypes=[("Images", "*.png *.jpg *.jpeg *.gif *.bmp *.webp")]
        )
        if not fp:
            return
        try:
            with open(fp, "rb") as f:
                raw = f.read()
            if fp.lower().endswith(".gif"):
                b64 = base64.b64encode(raw).decode()
            else:
                img = Image.open(io.BytesIO(raw)).convert("RGB")
                img = img.resize((800, 240), Image.Resampling.LANCZOS)
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                b64 = base64.b64encode(buf.getvalue()).decode()

            self.banner = b64
            self.client.send({"type": "update_banner", "banner": b64})
            if hasattr(self, "_banner_frame") and self._banner_frame.winfo_exists():
                self._refresh_banner_preview()
        except Exception as err:
            print(f"Banner upload error: {err}")

    def _refresh_own_avatar(self):
        if hasattr(self, "_own_avatar") and self._own_avatar.winfo_exists():
            parent = self._own_avatar.master
            self._own_avatar.stop_animation()
            self._own_avatar.destroy()
            self._own_avatar = AvatarWidget(
                parent, username=self.username or "?",
                b64_data=self.profile_picture, size=40
            )
            self._own_avatar.place(x=12, rely=0.5, anchor="w")
            self._own_avatar.start_animation()

    def _save_display_name(self):
        val = self._dn_entry.get().strip()
        if val:
            self.display_name = val
            self.client.send({"type": "update_display_name", "display_name": val})
            if hasattr(self, "_footer_name"):
                self._footer_name.configure(text=val)

    def _save_bio(self):
        val = self._bio_box.get("1.0", "end").strip()
        self.bio = val
        self.client.send({"type": "update_bio", "bio": val})
        self._save_bio_btn.configure(text="✓ Saved!", fg_color=SUCCESS)
        self.after(1800, lambda: self._save_bio_btn.configure(text="Save Bio", fg_color=ACCENT))

    def _show_user_profile(self, username: Optional[str]):
        if not username:
            return
        self.client.send({"type": "get_profile", "user": username})

    def _restore_chat_area(self):
        if self._saved_chat_widgets:
            if self._profile_view_frame is not None and self._profile_view_frame.winfo_exists():
                self._profile_view_frame.destroy()
            self._msg_scroll = self._saved_chat_widgets["msg_scroll"]
            self._input_bar = self._saved_chat_widgets["input_bar"]
            self._msg_scroll.pack(fill="both", expand=True)
            self._input_bar.pack(fill="x", side="bottom")
            if self._saved_chat_widgets.get("attachment_frame") and self._attachment_frame is not None and self._attachment_frame.winfo_ismapped():
                self._attachment_frame.pack(fill="x", side="bottom", before=self._input_bar)
            self._saved_chat_widgets = None
            if self.current_chat and self.current_chat in self.chat_buttons:
                self._select_chat(self.current_chat)

    def _toggle_block_from_profile(self, username: str):
        if username in self.blocked_users:
            self.client.send({"type": "unblock_user", "user": username})
        else:
            self.client.send({"type": "block_user", "user": username})
        self.after(500, lambda: self.client.send({"type": "get_profile", "user": username}))

    def _display_user_profile(self, data: dict):
        self._close_panel()

        if self._saved_chat_widgets is None:
            self._saved_chat_widgets = {
                "msg_scroll": self._msg_scroll,
                "input_bar": self._input_bar,
                "attachment_frame": self._attachment_frame if self._attachment_frame is not None and self._attachment_frame.winfo_ismapped() else None,
            }
            self._msg_scroll.pack_forget()
            self._input_bar.pack_forget()
            if self._attachment_frame is not None and self._attachment_frame.winfo_ismapped():
                self._attachment_frame.pack_forget()

        if self._profile_view_frame is not None and self._profile_view_frame.winfo_exists():
            self._profile_view_frame.destroy()

        profile_frame = ctk.CTkFrame(self._right, fg_color=BG_DARK, corner_radius=0)
        profile_frame.pack(fill="both", expand=True)
        self._profile_view_frame = profile_frame

        header = ctk.CTkFrame(profile_frame, fg_color=BG_PANEL, height=60, corner_radius=0)
        header.pack(fill="x")
        header.pack_propagate(False)

        back_btn = ctk.CTkButton(
            header, text="← Back", width=80, height=38, corner_radius=8,
            fg_color=BG_INPUT, hover_color=BG_HOVER,
            text_color=TEXT_PRI, font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            command=self._restore_chat_area
        )
        back_btn.place(x=12, rely=0.5, anchor="w")

        ctk.CTkLabel(
            header, text="User Profile",
            font=ctk.CTkFont(family=FONT_FAMILY, size=15, weight="bold"),
            text_color=TEXT_PRI
        ).place(relx=0.5, rely=0.5, anchor="center")

        scroll = ctk.CTkScrollableFrame(profile_frame, fg_color=BG_DARK,
                                        scrollbar_button_color=BG_HOVER)
        scroll.pack(fill="both", expand=True)

        username = data.get("username", "")
        display_name = data.get("display_name") or username
        profile_picture = data.get("profile_picture")
        banner = data.get("banner")
        bio = data.get("bio")
        created_at = data.get("created_at")

        if banner:
            banner_h = ctk.CTkFrame(scroll, fg_color=BG_INPUT, corner_radius=0, height=140)
            banner_h.pack(fill="x")
            banner_h.pack_propagate(False)
            frames = load_gif_frames(banner)
            if frames:
                resized = [(f.resize((banner_h.winfo_width(), 140), Image.Resampling.LANCZOS), d) for f, d in frames]
                AnimatedLabel(banner_h, resized).pack(expand=True, fill="both")
            else:
                try:
                    bd = base64.b64decode(banner)
                    img = Image.open(io.BytesIO(bd)).resize((800, 140), Image.Resampling.LANCZOS)
                    ctk_img = ctk.CTkImage(img, size=(800, 140))
                    ctk.CTkLabel(banner_h, image=ctk_img, text="").pack(expand=True)
                except Exception:
                    pass

        info_row = ctk.CTkFrame(scroll, fg_color="transparent")
        info_row.pack(fill="x", padx=24, pady=(24, 12))

        av = AvatarWidget(info_row, username=username, b64_data=profile_picture, size=90)
        av.pack(side="left", padx=(0, 24))
        av.start_animation()

        name_frame = ctk.CTkFrame(info_row, fg_color="transparent")
        name_frame.pack(side="left", fill="x", expand=True)

        ctk.CTkLabel(
            name_frame, text=display_name,
            font=ctk.CTkFont(family=FONT_FAMILY, size=22, weight="bold"),
            text_color=ACCENT_GLOW, anchor="w"
        ).pack(fill="x")
        ctk.CTkLabel(
            name_frame, text=f"@{username}",
            font=ctk.CTkFont(family=FONT_FAMILY, size=14),
            text_color=TEXT_SEC, anchor="w"
        ).pack(fill="x")

        self._divider(scroll, pady=18)

        if bio:
            self._section_label(scroll, "BIO")
            ctk.CTkLabel(
                scroll, text=bio,
                font=ctk.CTkFont(family=FONT_FAMILY, size=13),
                text_color=TEXT_PRI, anchor="w", wraplength=650, justify="left"
            ).pack(fill="x", padx=24, pady=(0, 18))
            self._divider(scroll, pady=10)

        if created_at:
            self._section_label(scroll, "MEMBER SINCE")
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(created_at)
                ctk.CTkLabel(
                    scroll, text=dt.strftime("%d %B %Y"),
                    font=ctk.CTkFont(family=FONT_FAMILY, size=13, weight="bold"),
                    text_color=TEXT_PRI, anchor="w"
                ).pack(fill="x", padx=24, pady=(0, 18))
            except Exception:
                pass

        is_blocked = username in self.blocked_users
        block_btn = ctk.CTkButton(
            scroll, text="Unblock User" if is_blocked else "Block User",
            height=44, corner_radius=10,
            fg_color=SUCCESS if is_blocked else DANGER,
            hover_color=SUCCESS if is_blocked else DANGER_HOVER,
            font=ctk.CTkFont(family=FONT_FAMILY, size=13, weight="bold"),
            command=lambda: self._toggle_block_from_profile(username)
        )
        block_btn.pack(pady=24)

    def render_chat_list(self, chats: List[dict]):
        for w in self._chat_list.winfo_children():
            w.destroy()
        self.chat_buttons.clear()
        for c in chats:
            self._add_chat_row(c)

    def _add_chat_row(self, user_data: dict):
        username = user_data.get("username", "")
        if not username or username in self.chat_buttons:
            return

        display = user_data.get("display_name") or username
        pic = user_data.get("profile_picture")
        last_msg = user_data.get("last_message", "")

        row = ctk.CTkFrame(self._chat_list, fg_color="transparent", corner_radius=10)
        row.pack(fill="x", pady=2, padx=3)

        av = AvatarWidget(row, username=username, b64_data=pic, size=44)
        av.pack(side="left", padx=(8, 10), pady=8)
        av.start_animation()

        text_frame = ctk.CTkFrame(row, fg_color="transparent")
        text_frame.pack(side="left", fill="both", expand=True)

        name_lbl = ctk.CTkLabel(
            text_frame, text=display,
            font=ctk.CTkFont(family=FONT_FAMILY, size=13, weight="bold"),
            text_color=ACCENT_GLOW, anchor="w"
        )
        name_lbl.pack(fill="x", pady=(6, 0))

        last_msg = last_msg or ""
        preview = (last_msg[:36] + "…") if len(last_msg) > 36 else last_msg
        prev_lbl = ctk.CTkLabel(
            text_frame, text=preview,
            font=ctk.CTkFont(family=FONT_FAMILY, size=11),
            text_color=TEXT_SEC, anchor="w"
        )
        prev_lbl.pack(fill="x", pady=(0, 4))

        for widget in (row, av, text_frame, name_lbl, prev_lbl):
            widget.bind("<Button-1>", lambda _e, u=username: self._select_chat(u))  # pylint: disable=unused-argument
        row.bind("<Enter>", lambda _e: row.configure(fg_color=BG_HOVER))  # pylint: disable=unused-argument
        row.bind("<Leave>", lambda _e: row.configure(  # pylint: disable=unused-argument
            fg_color=ACCENT if self.current_chat == username else "transparent"
        ))

        self.chat_buttons[username] = (row, name_lbl)

    def _select_chat(self, user: str):
        self.current_chat = user
        self._inline_menu.place_forget()
        self._clear_attachment_preview()

        display = user
        if user in self.chat_buttons:
            _, lbl = self.chat_buttons[user]
            display = lbl.cget("text")
        self._chat_hdr_name.configure(text=f"# {display}")

        for u, (row, _) in self.chat_buttons.items():
            row.configure(fg_color=BG_HOVER if u == user else "transparent")

        if hasattr(self, "_welcome") and self._welcome.winfo_exists():
            self._welcome.destroy()

        for w in self._msg_scroll.winfo_children():
            w.destroy()

        self.client.send({"type": "get_chat", "user": user, "limit": 50, "offset": 0})

    def _append_bubble(self, sender: str, msg: str, is_me: bool, ts: Optional[str], 
                      profile_pic: Optional[str] = None, msg_type: str = "text",
                      file_name: Optional[str] = None, file_size: Optional[int] = None, 
                      file_mime: Optional[str] = None, file_data: Optional[str] = None):

        display_name = (self.display_name or self.username) if is_me else sender
        pic = (self.profile_picture if is_me else profile_pic)
        time_str = fmt_time(ts)

        row = ctk.CTkFrame(self._msg_scroll, fg_color="transparent")
        row.pack(fill="x", padx=16, pady=(3, 8))

        av_frame = ctk.CTkFrame(row, fg_color="transparent", width=48, height=48)
        av_frame.pack(side="left", anchor="n", padx=(0, 12), pady=(3, 0))
        av_frame.pack_propagate(False)

        av = AvatarWidget(av_frame, username=display_name or "?", b64_data=pic, size=44)
        av.place(relx=0.5, rely=0.5, anchor="center")
        av.start_animation()

        content = ctk.CTkFrame(row, fg_color="transparent")
        content.pack(side="left", fill="both", expand=True)

        hdr = ctk.CTkFrame(content, fg_color="transparent", height=22)
        hdr.pack(fill="x")
        hdr.pack_propagate(False)

        ctk.CTkLabel(
            hdr, text=display_name or "?",
            font=ctk.CTkFont(family=FONT_FAMILY, size=13, weight="bold"),
            text_color=ACCENT_GLOW
        ).pack(side="left", padx=(0, 8))

        ctk.CTkLabel(
            hdr, text=time_str,
            font=ctk.CTkFont(family=FONT_FAMILY, size=10),
            text_color=TEXT_MUTED
        ).pack(side="left")

        if msg_type in ("image", "gif") and file_data:
            self._render_image_bubble(content, file_data, file_name)
        elif msg_type in ("video", "audio", "file", "application", "zip") and file_data:
            self._render_file_bubble(content, file_name, file_size, file_mime, file_data)
        else:
            bg = MSG_BG_ME if is_me else MSG_BG_OTHER
            msg_lbl = ctk.CTkLabel(
                content, text=msg,
                font=ctk.CTkFont(family=FONT_FAMILY, size=13),
                text_color=TEXT_PRI,
                wraplength=560, anchor="w", justify="left",
                fg_color=bg, corner_radius=8
            )
            msg_lbl.pack(fill="x", anchor="w", pady=(2, 0),
                         padx=(6, 0) if is_me else 0, ipadx=10, ipady=6)

        self.after(20, lambda: self._msg_scroll._parent_canvas.yview_moveto(1.0))

    def _render_image_bubble(self, parent, file_data: str, file_name: Optional[str]):
        try:
            raw = base64.b64decode(file_data)
            img_obj = Image.open(io.BytesIO(raw))

            if hasattr(img_obj, 'n_frames') and img_obj.n_frames > 1:
                frames = []
                for frame in ImageSequence.Iterator(img_obj):
                    f = frame.copy().convert("RGBA")
                    f.thumbnail((420, 320), Image.Resampling.LANCZOS)
                    frames.append((f, frame.info.get("duration", 100)))
                w, h = frames[0][0].size
                lbl = AnimatedLabel(parent, frames, width=w, height=h)
                lbl.pack(anchor="w", pady=(3, 0))
                self._bind_context_menu(lbl, file_name, None, file_data)
            else:
                img_obj.thumbnail((420, 320), Image.Resampling.LANCZOS)
                ctk_img = ctk.CTkImage(img_obj, size=img_obj.size)
                lbl = ctk.CTkLabel(parent, image=ctk_img, text="",
                                   width=img_obj.width, height=img_obj.height,
                                   cursor="hand2")
                lbl.pack(anchor="w", pady=(3, 0))
                lbl.bind("<Button-1>", lambda _e: self._show_enlarged_image(file_data, file_name))  # pylint: disable=unused-argument
                self._bind_context_menu(lbl, file_name, None, file_data)
        except Exception:
            ctk.CTkLabel(parent, text=f"[Image: {file_name}]",
                         text_color=ACCENT_GLOW).pack(anchor="w")

    def _render_file_bubble(self, parent, file_name: Optional[str], file_size: Optional[int], 
                           file_mime: Optional[str], file_data: str):  # pylint: disable=unused-argument
        card = ctk.CTkFrame(parent, fg_color=BG_HOVER, corner_radius=10)
        card.pack(anchor="w", pady=(4, 0))

        is_video = file_mime and file_mime.startswith("video")
        is_audio = file_mime and file_mime.startswith("audio")
        icon = "🎵" if is_audio else "🎬" if is_video else "📄"

        ctk.CTkLabel(
            card, text=f"{icon}  {file_name or 'file'}",
            font=ctk.CTkFont(family=FONT_FAMILY, size=12, weight="bold"),
            text_color=TEXT_PRI
        ).pack(side="left", padx=14, pady=12)

        ctk.CTkLabel(
            card, text=human_size(file_size),
            font=ctk.CTkFont(family=FONT_FAMILY, size=10),
            text_color=TEXT_SEC
        ).pack(side="left", padx=(0, 10))

        if is_video or is_audio:
            ctk.CTkButton(
                card, text="▶ Play", width=75, height=30, corner_radius=6,
                fg_color=ACCENT, hover_color=ACCENT_DARK,
                font=ctk.CTkFont(family=FONT_FAMILY, size=11),
                text_color="#fff",
                command=lambda: self._play_media(file_data, file_name)
            ).pack(side="right", padx=6, pady=10)

        ctk.CTkButton(
            card, text="⬇ Save", width=75, height=30, corner_radius=6,
            fg_color=ACCENT, hover_color=ACCENT_DARK,
            font=ctk.CTkFont(family=FONT_FAMILY, size=11),
            text_color="#fff",
            command=lambda: self._save_file(file_name, file_mime, file_data)
        ).pack(side="right", padx=6, pady=10)

        self._bind_context_menu(card, file_name, file_mime, file_data)

    def _play_media(self, file_data: str, file_name: Optional[str]):
        try:
            raw = base64.b64decode(file_data)
            suffix = os.path.splitext(file_name)[1] if file_name else ".bin"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(raw)
                tmp_path = tmp.name
            if platform.system() == "Windows":
                os.startfile(tmp_path)
            elif platform.system() == "Darwin":
                subprocess.run(["open", tmp_path], check=False)
            else:
                subprocess.run(["xdg-open", tmp_path], check=False)
            self.after(30000, lambda: os.unlink(tmp_path) if os.path.exists(tmp_path) else None)
        except Exception as err:
            print(f"Play error: {err}")

    def _bind_context_menu(self, widget, file_name: Optional[str], file_mime: Optional[str], file_data: str):
        def show_menu(event):
            menu = ctk.CTkToplevel(self)
            menu.overrideredirect(True)
            menu.configure(fg_color=BG_CARD)
            menu.geometry(f"160x44+{event.x_root}+{event.y_root}")
            menu.lift()
            menu.attributes("-topmost", True)

            ctk.CTkButton(
                menu, text="⬇ Download",
                fg_color="transparent", hover_color=BG_HOVER,
                text_color=TEXT_PRI, height=36, corner_radius=6,
                command=lambda: (menu.destroy(), self._save_file(file_name, file_mime, file_data))
            ).pack(fill="x", padx=4, pady=4)

            menu.after(100, menu.focus_force)
            self.after(200, lambda: menu.bind("<FocusOut>", lambda _e: menu.destroy()))  # pylint: disable=unused-argument

        widget.bind("<Button-3>", show_menu)

    def _show_enlarged_image(self, file_data: str, file_name: Optional[str]):
        try:
            raw = base64.b64decode(file_data)
            img: Image.Image = Image.open(io.BytesIO(raw))

            modal = ctk.CTkToplevel(self)
            modal.title(file_name or "Image")
            modal.configure(fg_color=BG_DARK)

            screen_w = self.winfo_screenwidth()
            screen_h = self.winfo_screenheight()

            max_w = int(screen_w * 0.8)
            max_h = int(screen_h * 0.8)

            img_w, img_h = img.size
            scale = min(max_w / img_w, max_h / img_h, 1.0)
            if scale < 1.0:
                new_w = int(img_w * scale)
                new_h = int(img_h * scale)
                img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)

            win_w = min(img.width + 40, screen_w)
            win_h = min(img.height + 90, screen_h)
            modal.geometry(f"{win_w}x{win_h}")

            x = (screen_w - win_w) // 2
            y = (screen_h - win_h) // 2
            modal.geometry(f"+{x}+{y}")

            header = ctk.CTkFrame(modal, fg_color=BG_PANEL, height=44, corner_radius=0)
            header.pack(fill="x")
            header.pack_propagate(False)

            ctk.CTkLabel(header, text=file_name or "Image",
                         font=ctk.CTkFont(family=FONT_FAMILY, size=12, weight="bold"),
                         text_color=TEXT_PRI).pack(side="left", padx=16)

            ctk.CTkButton(header, text="✕", width=36, height=36, corner_radius=8,
                         fg_color="transparent", hover_color=DANGER,
                         text_color=TEXT_SEC, font=ctk.CTkFont(size=12),
                         command=modal.destroy).pack(side="right", padx=10)

            scroll = ctk.CTkScrollableFrame(modal, fg_color=BG_DARK)
            scroll.pack(fill="both", expand=True, padx=20, pady=12)

            ctk_img = ctk.CTkImage(img, size=(img.width, img.height))
            img_lbl = ctk.CTkLabel(scroll, image=ctk_img, text="")
            img_lbl.pack(expand=True)

            self._bind_context_menu(img_lbl, file_name, None, file_data)

            modal.grab_set()
            modal.lift()
            modal.focus_force()

        except Exception as err:
            print(f"Error showing enlarged image: {err}")

    def _save_file(self, file_name: Optional[str], file_mime: Optional[str], file_data: str):  # pylint: disable=unused-argument
        import tkinter.messagebox as mb
        path = filedialog.asksaveasfilename(initialfile=file_name or "download")
        if not path:
            return
        try:
            raw = base64.b64decode(file_data)
            with open(path, "wb") as f:
                f.write(raw)
            mb.showinfo("Saved", f"File saved to:\n{path}")
        except Exception as err:
            mb.showerror("Error", str(err))

    def _pick_file(self):
        if not self.current_chat:
            return
        fp = filedialog.askopenfilename(
            filetypes=[
                ("All files", "*.*"),
                ("Images", "*.png *.jpg *.jpeg *.gif *.bmp *.webp"),
                ("Videos", "*.mp4 *.mov *.avi *.mkv"),
                ("Audio", "*.mp3 *.wav *.ogg *.flac *.m4a"),
                ("Documents", "*.pdf *.docx *.xlsx *.pptx *.txt"),
                ("Archives", "*.zip *.rar *.7z *.tar *.gz"),
            ]
        )
        if not fp:
            return

        size = os.path.getsize(fp)
        if size > MAX_FILE_SIZE:
            import tkinter.messagebox as mb
            mb.showerror("File too large", "Maximum file size is 2 GB.")
            return

        mime, _ = mimetypes.guess_type(fp)
        mime = mime or "application/octet-stream"
        file_name = os.path.basename(fp)

        self.pending_attachment = {
            "path": fp,
            "name": file_name,
            "size": size,
            "mime": mime,
            "data": None,
        }
        self._show_attachment_preview(file_name, size, mime, fp)

    def _show_attachment_preview(self, file_name: str, file_size: int, mime: str, _file_path: str):  # pylint: disable=unused-argument
        self._clear_attachment_preview()

        if self._attachment_frame is not None and not self._attachment_frame.winfo_ismapped():
            self._attachment_frame.pack(fill="x", side="bottom", before=self._input_bar)
            self._attachment_frame.configure(height=80)
            self._attachment_frame.pack_propagate(False)

        inner = ctk.CTkFrame(self._attachment_frame, fg_color=BG_INPUT, corner_radius=10)
        inner.pack(fill="both", expand=True, padx=12, pady=10)

        icon_text = "🖼️" if mime.startswith("image") else "🎬" if mime.startswith("video") else "🎵" if mime.startswith("audio") else "📎"
        ctk.CTkLabel(inner, text=icon_text, font=ctk.CTkFont(size=24)).pack(side="left", padx=10)

        info = ctk.CTkFrame(inner, fg_color="transparent")
        info.pack(side="left", fill="x", expand=True, padx=10)
        ctk.CTkLabel(info, text=file_name,
                     font=ctk.CTkFont(family=FONT_FAMILY, size=12, weight="bold"),
                     text_color=TEXT_PRI, anchor="w").pack(fill="x")
        ctk.CTkLabel(info, text=human_size(file_size),
                     font=ctk.CTkFont(family=FONT_FAMILY, size=10),
                     text_color=TEXT_SEC, anchor="w").pack(fill="x")

        ctk.CTkButton(inner, text="✕", width=36, height=36, corner_radius=8,
                      fg_color="transparent", hover_color=DANGER,
                      text_color=TEXT_SEC, font=ctk.CTkFont(size=12),
                      command=self._clear_attachment_preview).pack(side="right", padx=10)

    def _clear_attachment_preview(self):
        self.pending_attachment = None
        if self._attachment_frame and self._attachment_frame.winfo_ismapped():
            for w in self._attachment_frame.winfo_children():
                w.destroy()
            self._attachment_frame.pack_forget()

    def _send_msg(self):
        if not self.current_chat:
            return
        msg = self._msg_entry.get().strip()

        if self.pending_attachment:
            att = self.pending_attachment
            self._clear_attachment_preview()

            with open(att["path"], "rb") as f:
                raw = f.read()
            b64_data = base64.b64encode(raw).decode()

            transfer_id = secrets.token_hex(8)
            self._start_file_upload(transfer_id, att["name"], att["size"], att["mime"], b64_data, msg)
            self._msg_entry.delete(0, "end")
        elif msg:
            self.client.send({"type": "message", "to": self.current_chat, "msg": msg})
            from datetime import datetime
            self._append_bubble(
                self.username or "Me", msg, True,
                datetime.utcnow().isoformat()
            )
            self._msg_entry.delete(0, "end")

    def _start_file_upload(self, transfer_id: str, file_name: str, file_size: int, 
                          file_mime: str, file_b64: str, optional_msg: str):
        prog_row = ctk.CTkFrame(self._msg_scroll, fg_color="transparent")
        prog_row.pack(fill="x", padx=16, pady=5)
        prog_lbl = ctk.CTkLabel(
            prog_row,
            text=f"⬆ Uploading {file_name} ({human_size(file_size)})…",
            font=ctk.CTkFont(family=FONT_FAMILY, size=12),
            text_color=TEXT_SEC
        )
        prog_lbl.pack(side="left")
        prog_bar = ctk.CTkProgressBar(prog_row, width=200, height=8, corner_radius=4,
                                       fg_color=BG_INPUT, progress_color=ACCENT)
        prog_bar.pack(side="left", padx=12)
        prog_bar.set(0)

        def cancel():
            if transfer_id in self._pending_transfers:
                self._pending_transfers[transfer_id]["cancelled"] = True
                tf = self._pending_transfers.pop(transfer_id)
                if tf.get("prog_row") and tf["prog_row"].winfo_exists():
                    tf["prog_row"].destroy()

        ctk.CTkButton(
            prog_row, text="✕", width=30, height=30, corner_radius=6,
            fg_color="transparent", hover_color=DANGER,
            text_color=TEXT_SEC, font=ctk.CTkFont(size=12),
            command=cancel
        ).pack(side="left", padx=6)

        self._pending_transfers[transfer_id] = {
            "prog_row": prog_row,
            "prog_lbl": prog_lbl,
            "prog_bar": prog_bar,
            "file_name": file_name,
            "file_size": file_size,
            "file_mime": file_mime,
            "cancelled": False,
            "ready_set": set(),
        }

        def upload_thread():
            try:
                total_len = len(file_b64)
                chunk_b64_size = (CHUNK_SIZE * 4) // 3
                chunks = [file_b64[i:i+chunk_b64_size] for i in range(0, total_len, chunk_b64_size)]
                total_chunks = len(chunks)

                self.client.send({
                    "type": "file_start",
                    "to": self.current_chat,
                    "transfer_id": transfer_id,
                    "file_name": file_name,
                    "file_size": file_size,
                    "file_mime": file_mime,
                    "total_chunks": total_chunks,
                    "msg": optional_msg,
                })

                timeout = time.time() + 15
                while transfer_id not in self._pending_transfers.get(transfer_id, {}).get("ready_set", set()):
                    time.sleep(0.05)
                    if time.time() > timeout:
                        break

                for i, chunk in enumerate(chunks):
                    if transfer_id in self._pending_transfers and self._pending_transfers[transfer_id].get("cancelled"):
                        return
                    self.client.send({
                        "type": "file_chunk",
                        "transfer_id": transfer_id,
                        "chunk_index": i,
                        "data": chunk,
                    })
                    progress = (i + 1) / total_chunks
                    self.after(0, lambda p=progress, tid=transfer_id: self._update_upload_progress(tid, p))
                    time.sleep(0.002)
            except Exception as err:
                print(f"Upload error: {err}")

        threading.Thread(target=upload_thread, daemon=True).start()

    def _update_upload_progress(self, transfer_id: str, progress: float):
        if transfer_id not in self._pending_transfers:
            return
        tf = self._pending_transfers[transfer_id]
        if tf.get("prog_bar") and tf["prog_bar"].winfo_exists():
            tf["prog_bar"].set(progress)
        if tf.get("prog_lbl") and tf["prog_lbl"].winfo_exists():
            tf["prog_lbl"].configure(
                text=f"⬆ {tf['file_name']} ({int(progress*100)}%)"
            )

    def _on_search(self, _event=None):  # pylint: disable=unused-argument
        q = self._search_entry.get().strip()
        if len(q) >= 1:
            self.client.send({"type": "search", "query": q})
        elif q == "":
            self.client.send({"type": "load_chats"})

    def _logout(self):
        tok = self.session_token or self._load_token()
        if tok:
            self.client.send({"type": "logout", "token": tok})
        self._clear_token()
        self.session_token = None
        self.username = None
        self.display_name = None
        self.profile_picture = None
        self.current_chat = None
        self.chat_buttons = {}
        self._clear_attachment_preview()
        self._build_auth("login")

    def handle_server(self, data: dict):
        t = data.get("type")

        if t == "error":
            if hasattr(self, "_status") and self._status.winfo_exists():
                self._status.configure(text=data.get("msg", "Error"))
            return

        if t == "register_success":
            if hasattr(self, "_status") and self._status.winfo_exists():
                self._status.configure(text="✓ Account created!", text_color=SUCCESS)
            self.after(900, lambda: self._build_auth("login"))
            return

        if t == "login_success":
            self.username = data.get("username")
            self.display_name = data.get("display_name")
            self.profile_picture = data.get("profile_picture")
            self.banner = data.get("banner")
            self.bio = data.get("bio")
            self.created_at = data.get("created_at")
            self.blocked_users = set(data.get("blocked_users", []))
            tok = data.get("session_token")
            if tok:
                self.session_token = tok
                self._save_token(tok)
            self._build_chat_ui()
            return

        if t == "token_invalid":
            self._clear_token()
            return

        if t == "logout_success":
            return

        if t == "profile_updated":
            if "display_name" in data:
                self.display_name = data["display_name"]
                if hasattr(self, "_footer_name"):
                    self._footer_name.configure(text=data["display_name"])
            if "profile_picture" in data:
                self.profile_picture = data["profile_picture"]
                self._refresh_own_avatar()
            if "banner" in data:
                self.banner = data["banner"]
            if "bio" in data:
                self.bio = data["bio"]
            return

        if not hasattr(self, "_chat_list"):
            return

        if t == "chat_list":
            self.render_chat_list(data.get("chats", []))

        elif t == "search_results":
            self.render_chat_list(data.get("results", []))

        elif t == "chat_history":
            if data.get("user") == self.current_chat:
                for w in self._msg_scroll.winfo_children():
                    w.destroy()
                for item in data.get("messages", []):
                    if isinstance(item, dict):
                        sender = item.get("sender")
                        msg = item.get("message", "")
                        ts = item.get("timestamp")
                        msg_type = item.get("type", "text")
                        is_me = sender == self.username
                        self._append_bubble(
                            sender, msg, is_me, ts,
                            msg_type=msg_type,
                            file_name=item.get("file_name"),
                            file_size=item.get("file_size"),
                            file_mime=item.get("file_mime"),
                            file_data=item.get("file_data"),
                        )
                    else:
                        sender = item[0] if len(item) > 0 else ""
                        msg = item[1] if len(item) > 1 else ""
                        ts = None
                        is_me = sender == self.username
                        self._append_bubble(sender, msg, is_me, ts)

        elif t == "message":
            sender = data.get("from")
            if sender in self.blocked_users:
                return
            self._ensure_chat(sender)
            if sender == self.current_chat:
                self._append_bubble(
                    sender, data.get("msg", ""), False,
                    data.get("timestamp"),
                    msg_type=data.get("msg_type", "text")
                )

        elif t == "file_message":
            sender = data.get("from")
            if sender in self.blocked_users:
                return
            self._ensure_chat(sender)
            if sender == self.current_chat:
                mime = data.get("file_mime", "")
                if mime.startswith("image"):
                    msg_type = "image"
                elif mime.startswith("video"):
                    msg_type = "video"
                elif mime.startswith("audio"):
                    msg_type = "audio"
                else:
                    msg_type = "file"
                self._append_bubble(
                    sender, data.get("msg", ""), False,
                    data.get("timestamp"), msg_type=msg_type,
                    file_name=data.get("file_name"),
                    file_size=data.get("file_size"),
                    file_mime=data.get("file_mime"),
                    file_data=data.get("file_data"),
                )

        elif t == "file_ready":
            tid = data.get("transfer_id")
            if tid in self._pending_transfers:
                self._pending_transfers[tid]["ready_set"] = {tid}

        elif t == "file_chunk_ack":
            tid = data.get("transfer_id")
            received = data.get("received", 0)
            total = data.get("total", 1)
            progress = received / total
            self._update_upload_progress(tid, progress)

        elif t == "file_complete":
            tid = data.get("transfer_id", "")
            if tid in self._pending_transfers:
                tf = self._pending_transfers.pop(tid)
                if tf.get("prog_row") and tf["prog_row"].winfo_exists():
                    tf["prog_row"].destroy()
            mime = data.get("file_mime", "")
            if mime.startswith("image"):
                msg_type = "image"
            elif mime.startswith("video"):
                msg_type = "video"
            elif mime.startswith("audio"):
                msg_type = "audio"
            else:
                msg_type = "file"
            from datetime import datetime
            self._append_bubble(
                self.username or "Me", data.get("msg", ""), True,
                data.get("timestamp", datetime.utcnow().isoformat()),
                msg_type=msg_type,
                file_name=data.get("file_name"),
                file_size=data.get("file_size"),
                file_mime=data.get("file_mime"),
                file_data=data.get("file_data"),
            )

        elif t == "profile_data":
            self._display_user_profile(data)

        elif t == "block_success":
            user = data.get("user")
            if user:
                self.blocked_users.add(user)

        elif t == "unblock_success":
            user = data.get("user")
            if user:
                self.blocked_users.discard(user)

    def _ensure_chat(self, user: str):
        if user not in self.chat_buttons and hasattr(self, "_chat_list"):
            self._add_chat_row({"username": user, "display_name": None, "profile_picture": None})

if __name__ == "__main__":
    chat_app = App()
    chat_app.mainloop()