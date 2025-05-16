import flet as ft
from flet import icons
import sqlite3
import datetime as dt
import csv
import json
import os
import shutil
import cv2
import numpy as np
import qrcode
import uuid
import base64
import logging
from logging.handlers import RotatingFileHandler
import zipfile
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from functools import wraps
import threading

# Configuración de logging
logging.basicConfig(
    handlers=[RotatingFileHandler('inv.log', maxBytes=10*1024*1024, backupCount=5)],
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "worker": {"password": "worker123", "role": "worker"}
}

@dataclass
class QRData:
    tool_uuid: str
    i_id: int
    name: str
    date: str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    uuid: str = str(uuid.uuid4())
    def to_json(self):
        return json.dumps({
            "tool_uuid": self.tool_uuid,
            "i_id": self.i_id,
            "name": self.name,
            "date": self.date,
            "uuid": self.uuid
        })

@dataclass
class RetData:
    h_id: int
    i_id: int
    worker: str
    date: str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    notes: str = ""
    def to_dict(self):
        return {
            "h_id": self.h_id,
            "i_id": self.i_id,
            "worker": self.worker,
            "date": self.date,
            "notes": self.notes
        }

@dataclass
class ToolInst:
    id: int
    h_id: int
    tool_uuid: str
    serial: str
    status: str
    qr_uuid: str
    img: Optional[str] = None

@dataclass
class Tool:
    id: int
    tool_uuid: str
    name: str
    resp: str
    qty: int
    is_consumable: bool
    img: Optional[str] = None
    status: str = "avail"

class QRMgr:
    def __init__(self, conn: sqlite3.Connection, qr_dir: str = "qr_codes"):
        self.conn, self.c = conn, conn.cursor()
        self.qr_dir = os.path.abspath(qr_dir)
        os.makedirs(self.qr_dir, exist_ok=True)
        self._init_db()

    def _init_db(self):
        self.c.executescript('''
        CREATE TABLE IF NOT EXISTS h_qr (
            id INTEGER PRIMARY KEY,
            tool_uuid TEXT,
            i_id INTEGER,
            qr_uuid TEXT UNIQUE,
            date TEXT,
            img TEXT,
            FOREIGN KEY (tool_uuid) REFERENCES tools (tool_uuid) ON DELETE CASCADE,
            FOREIGN KEY (i_id) REFERENCES tool_inst (id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS rets (
            id INTEGER PRIMARY KEY,
            h_id INTEGER,
            i_id INTEGER,
            worker TEXT,
            date TEXT,
            notes TEXT,
            FOREIGN KEY (h_id) REFERENCES tools (id) ON DELETE CASCADE,
            FOREIGN KEY (i_id) REFERENCES tool_inst (id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS tool_inst (
            id INTEGER PRIMARY KEY,
            h_id INTEGER,
            tool_uuid TEXT,
            serial TEXT UNIQUE,
            status TEXT,
            qr_uuid TEXT UNIQUE,
            img TEXT,
            FOREIGN KEY (h_id) REFERENCES tools (id) ON DELETE CASCADE,
            FOREIGN KEY (tool_uuid) REFERENCES tools (tool_uuid) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_ti_h_id ON tool_inst(h_id);
        CREATE INDEX IF NOT EXISTS idx_ti_uuid ON tool_inst(tool_uuid);
        CREATE INDEX IF NOT EXISTS idx_loans_h_id ON loans(h_id);
        CREATE INDEX IF NOT EXISTS idx_rets_h_id ON rets(h_id);
        ''')
        self.c.execute("PRAGMA table_info(tools)")
        cols = [col[1] for col in self.c.fetchall()]
        for col, sql in [
            ('status', 'ALTER TABLE tools ADD COLUMN status TEXT DEFAULT "avail"'),
            ('resp', 'ALTER TABLE tools ADD COLUMN resp TEXT'),
            ('tool_uuid', 'ALTER TABLE tools ADD COLUMN tool_uuid TEXT UNIQUE'),
            ('is_consumable', 'ALTER TABLE tools ADD COLUMN is_consumable BOOLEAN DEFAULT 0')
        ]:
            if col not in cols:
                self.c.execute(sql)
        self.conn.commit()

    def gen_qr(self, tool_uuid: str, i_id: int, name: str) -> Optional[str]:
        try:
            self.c.execute('SELECT qr_uuid, img FROM h_qr WHERE tool_uuid = ? AND i_id = ?', (tool_uuid, i_id))
            existing = self.c.fetchone()
            if existing and existing[1] and os.path.exists(existing[1]):
                return existing[1]
            qr_data = QRData(tool_uuid=tool_uuid, i_id=i_id, name=name)
            qr = qrcode.QRCode(
                version=1,
                error_correction=qrcode.constants.ERROR_CORRECT_H,
                box_size=10,
                border=4
            )
            qr.add_data(qr_data.to_json())
            qr.make(fit=True)
            qr_img = qr.make_image(fill_color="black", back_color="white")
            qr_file = f"qr_{tool_uuid}_{i_id}_{qr_data.uuid}.png"
            qr_path = os.path.join(self.qr_dir, qr_file)
            qr_img.save(qr_path)
            if existing:
                self.c.execute('UPDATE h_qr SET img = ? WHERE tool_uuid = ? AND i_id = ?', (qr_path, tool_uuid, i_id))
            else:
                self.c.execute(
                    'INSERT INTO h_qr (tool_uuid, i_id, qr_uuid, date, img) VALUES (?, ?, ?, ?, ?)',
                    (tool_uuid, i_id, qr_data.uuid, qr_data.date, qr_path)
                )
            self.conn.commit()
            return qr_path
        except Exception as e:
            logger.error("QR gen err: %s", e)
            return None

    def read_qr(self, qr_json: str) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(qr_json)
            tool_uuid, i_id = data.get("tool_uuid"), data.get("i_id")
            if not (tool_uuid and i_id):
                return None
            self.c.execute('''
                SELECT h.id, h.name, h.resp, h.qty, h.img, h.status, h.is_consumable, ti.id, ti.serial, ti.status, ti.img
                FROM tools h JOIN tool_inst ti ON h.tool_uuid = ti.tool_uuid
                WHERE h.tool_uuid = ? AND ti.id = ?
            ''', (tool_uuid, i_id))
            r = self.c.fetchone()
            if not r:
                return None
            return {
                "id": r[0],
                "name": r[1],
                "resp": r[2],
                "qty": r[3],
                "img": r[4],
                "status": r[5],
                "is_consumable": bool(r[6]),
                "i_id": r[7],
                "serial": r[8],
                "i_status": r[9],
                "i_img": r[10],
                "qr_uuid": data.get("uuid")
            }
        except Exception as e:
            logger.error("QR read err: %s", e)
            return None

    def reg_ret(self, ret: RetData) -> bool:
        try:
            self.c.execute(
                'INSERT INTO rets (h_id, i_id, worker, date, notes) VALUES (?, ?, ?, ?, ?)',
                (ret.h_id, ret.i_id, ret.worker, ret.date, ret.notes)
            )
            self.c.execute('UPDATE tool_inst SET status = "avail" WHERE id = ? AND h_id = ?', (ret.i_id, ret.h_id))
            self.conn.commit()
            return True
        except Exception as e:
            logger.error("Ret reg err: %s", e)
            return False

    def get_stats(self, cache_secs: int = 60) -> Dict[str, Any]:
        if hasattr(self, '_cache') and (dt.datetime.now() - self._cache_time).seconds < cache_secs:
            return self._cache
        try:
            self.c.execute('SELECT COUNT(*) FROM tool_inst WHERE status = "loaned"')
            loaned = self.c.fetchone()[0] or 0
            self.c.execute('SELECT COUNT(*) FROM loans WHERE DATE(date) = DATE("now")')
            loans_today = self.c.fetchone()[0] or 0
            self.c.execute('SELECT COUNT(*) FROM rets WHERE DATE(date) = DATE("now")')
            rets_today = self.c.fetchone()[0] or 0
            self.c.execute('''
                SELECT h.name, COUNT(l.id)
                FROM loans l JOIN tools h ON l.h_id = h.id
                GROUP BY h.id, h.name
                ORDER BY COUNT(l.id) DESC LIMIT 5
            ''')
            pop_tools = [{"name": r[0], "loans": r[1]} for r in self.c.fetchall()]
            stats = {
                "loaned": loaned,
                "loans_today": loans_today,
                "rets_today": rets_today,
                "pop_tools": pop_tools,
                "ts": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            self._cache, self._cache_time = stats, dt.datetime.now()
            return stats
        except Exception as e:
            logger.error("Stats err: %s", e)
            return {
                "loaned": 0,
                "loans_today": 0,
                "rets_today": 0,
                "pop_tools": [],
                "ts": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

class InvApp:
    def __init__(self):
        self.conn = sqlite3.connect('inv.db', check_same_thread=False)
        self.c = self.conn.cursor()
        self._init_db()
        self.qr_mgr = QRMgr(self.conn)
        self.img_dir = os.path.abspath("tool_imgs")
        os.makedirs(self.img_dir, exist_ok=True)
        self._cache = None
        self._cache_time = None

    def _init_db(self):
        self.c.executescript('''
        CREATE TABLE IF NOT EXISTS tools (
            id INTEGER PRIMARY KEY,
            tool_uuid TEXT UNIQUE,
            name TEXT,
            resp TEXT,
            qty INTEGER CHECK(qty >= 0),
            is_consumable BOOLEAN DEFAULT 0,
            img TEXT,
            status TEXT DEFAULT "avail"
        );
        CREATE TABLE IF NOT EXISTS loans (
            id INTEGER PRIMARY KEY,
            h_id INTEGER,
            i_id INTEGER,
            worker TEXT,
            date TEXT,
            FOREIGN KEY (h_id) REFERENCES tools (id) ON DELETE CASCADE,
            FOREIGN KEY (i_id) REFERENCES tool_inst (id) ON DELETE CASCADE
        );
        ''')
        self.conn.commit()

    def add_tool(self, name: str, resp: str, qty: int, is_consumable: bool, img: Optional[str] = None) -> tuple[bool, str]:
        try:
            if not name.strip() or not resp.strip() or qty < 0:
                return False, "Invalid input"
            tool_uuid = str(uuid.uuid4())
            img_path = self._save_img(img) if img else None
            self.c.execute('''
                INSERT INTO tools (tool_uuid, name, resp, qty, is_consumable, img, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (tool_uuid, name, resp, qty, is_consumable, img_path, 'avail'))
            h_id = self.c.lastrowid
            if not is_consumable:
                for i in range(qty):
                    serial = f"{tool_uuid}-{i+1:03d}"
                    self.c.execute('''
                        INSERT INTO tool_inst (h_id, tool_uuid, serial, status, qr_uuid, img)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (h_id, tool_uuid, serial, 'avail', str(uuid.uuid4()), img_path))
                    i_id = self.c.lastrowid
                    self.qr_mgr.gen_qr(tool_uuid, i_id, name)
            self.conn.commit()
            self._cache = None
            return True, f"Tool '{name}' added"
        except sqlite3.Error as e:
            return False, f"DB err: {str(e)}"

    def consume_tool(self, id: int, qty: int) -> tuple[bool, str]:
        try:
            self.c.execute('SELECT name, qty, is_consumable FROM tools WHERE id = ?', (id,))
            r = self.c.fetchone()
            if not r:
                return False, "Tool not found"
            name, curr_qty, is_consumable = r
            if not is_consumable:
                return False, "Not consumable"
            if qty <= 0 or qty > curr_qty:
                return False, f"Invalid qty (max {curr_qty})"
            new_qty = curr_qty - qty
            self.c.execute('UPDATE tools SET qty = ? WHERE id = ?', (new_qty, id))
            self.conn.commit()
            self._cache = None
            return True, f"Consumed {qty} {name}"
        except sqlite3.Error as e:
            return False, f"DB err: {str(e)}"

    def get_tools(self) -> List[Tool]:
        if self._cache and self._cache_time and (dt.datetime.now() - self._cache_time).seconds < 60:
            return self._cache
        try:
            self.c.execute('''
                SELECT id, tool_uuid, name, resp, qty, is_consumable, img, status
                FROM tools ORDER BY name
            ''')
            tools = [Tool(id=r[0], tool_uuid=r[1], name=r[2], resp=r[3], qty=r[4], is_consumable=bool(r[5]), img=r[6], status=r[7]) for r in self.c.fetchall()]
            self._cache, self._cache_time = tools, dt.datetime.now()
            return tools
        except sqlite3.Error as e:
            logger.error("Get tools err: %s", e)
            return []

    def get_tool(self, id: int) -> Optional[Tool]:
        try:
            self.c.execute('''
                SELECT id, tool_uuid, name, resp, qty, is_consumable, img, status
                FROM tools WHERE id = ?
            ''', (id,))
            r = self.c.fetchone()
            return Tool(id=r[0], tool_uuid=r[1], name=r[2], resp=r[3], qty=r[4], is_consumable=bool(r[5]), img=r[6], status=r[7]) if r else None
        except sqlite3.Error as e:
            logger.error("Get tool err: %s", e)
            return None

    def get_inst(self, i_id: int) -> Optional[ToolInst]:
        try:
            self.c.execute('''
                SELECT id, h_id, tool_uuid, serial, status, qr_uuid, img
                FROM tool_inst WHERE id = ?
            ''', (i_id,))
            r = self.c.fetchone()
            return ToolInst(*r) if r else None
        except sqlite3.Error as e:
            logger.error("Get inst err: %s", e)
            return None

    def get_insts(self, h_id: int) -> List[ToolInst]:
        try:
            self.c.execute('''
                SELECT id, h_id, tool_uuid, serial, status, qr_uuid, img
                FROM tool_inst WHERE h_id = ? ORDER BY serial
            ''', (h_id,))
            return [ToolInst(*r) for r in self.c.fetchall()]
        except sqlite3.Error as e:
            logger.error("Get insts err: %s", e)
            return []

    def upd_tool(self, id: int, name: str, resp: str, qty: int, is_consumable: bool, img: Optional[str] = None) -> tuple[bool, str]:
        try:
            if not name.strip() or not resp.strip() or qty < 0:
                return False, "Invalid input"
            curr = self.get_tool(id)
            if not curr:
                return False, "Tool not found"
            img_path = self._save_img(img) if img else curr.img
            self.c.execute('''
                UPDATE tools
                SET name = ?, resp = ?, qty = ?, is_consumable = ?, img = ?
                WHERE id = ?
            ''', (name, resp, qty, is_consumable, img_path, id))
            if not is_consumable:
                curr_insts = len(self.get_insts(id))
                tool_uuid = curr.tool_uuid
                if qty > curr_insts:
                    for i in range(curr_insts + 1, qty + 1):
                        serial = f"{tool_uuid}-{i:03d}"
                        self.c.execute('''
                            INSERT INTO tool_inst (h_id, tool_uuid, serial, status, qr_uuid, img)
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', (id, tool_uuid, serial, 'avail', str(uuid.uuid4()), img_path))
                        self.qr_mgr.gen_qr(tool_uuid, self.c.lastrowid, name)
                elif qty < curr_insts:
                    self.c.execute('DELETE FROM tool_inst WHERE h_id = ? AND serial > ?', (id, f"{tool_uuid}-{qty:03d}"))
            else:
                self.c.execute('DELETE FROM tool_inst WHERE h_id = ?', (id,))
            self.conn.commit()
            self._cache = None
            return True, "Tool updated"
        except sqlite3.Error as e:
            return False, f"DB err: {str(e)}"

    def del_tool(self, id: int) -> tuple[bool, str]:
        try:
            self.c.execute('SELECT name, img FROM tools WHERE id = ?', (id,))
            r = self.c.fetchone()
            if not r:
                return False, "Tool not found"
            name, img = r
            self.c.execute('DELETE FROM tools WHERE id = ?', (id,))
            self.conn.commit()
            if img and os.path.exists(img):
                os.remove(img)
            self._cache = None
            return True, f"Tool '{name}' deleted"
        except sqlite3.Error as e:
            return False, f"Del err: {str(e)}"

    def _save_img(self, img_path: Optional[str]) -> Optional[str]:
        if not img_path or not os.path.exists(img_path):
            return None
        try:
            fname = f"{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(img_path)}"
            dest = os.path.join(self.img_dir, fname)
            shutil.copy(img_path, dest)
            return dest
        except Exception as e:
            logger.error("Save img err: %s", e)
            return None

    def regen_qr(self, tool_uuid: str, i_id: int, name: str) -> Optional[str]:
        try:
            self.c.execute('DELETE FROM h_qr WHERE tool_uuid = ? AND i_id = ?', (tool_uuid, i_id))
            qr_path = self.qr_mgr.gen_qr(tool_uuid, i_id, name)
            if qr_path:
                self.c.execute('UPDATE tool_inst SET qr_uuid = ? WHERE id = ?', (str(uuid.uuid4()), i_id))
                self.conn.commit()
                return qr_path
            return None
        except Exception as e:
            logger.error("Regen QR err: %s", e)
            return None

    def reg_loan(self, h_id: int, i_id: int, worker: str) -> bool:
        try:
            if not worker.strip():
                return False
            date = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute('''
                INSERT INTO loans (h_id, i_id, worker, date)
                VALUES (?, ?, ?, ?)
            ''', (h_id, i_id, worker, date))
            self.c.execute('UPDATE tool_inst SET status = "loaned" WHERE id = ? AND h_id = ?', (i_id, h_id))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            logger.error("Loan reg err: %s", e)
            return False

    def check_overdue(self) -> List[Dict[str, Any]]:
        try:
            limit = (dt.datetime.now().astimezone() - dt.timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
            self.c.execute('''
                SELECT l.id, h.name, ti.serial, l.worker, l.date
                FROM loans l
                JOIN tools h ON l.h_id = h.id
                JOIN tool_inst ti ON l.i_id = ti.id
                WHERE l.date < ? AND ti.status = "loaned"
            ''', (limit,))
            return [
                {
                    "id": r[0],
                    "tool": r[1],
                    "serial": r[2],
                    "worker": r[3],
                    "date": r[4],
                    "hrs_overdue": round(
                        (dt.datetime.now() - dt.datetime.strptime(r[4], "%Y-%m-%d %H:%M:%S")).total_seconds() / 3600, 2
                    )
                } for r in self.c.fetchall()
            ]
        except Exception as e:
            logger.error("Overdue err: %s", e)
            return []

    def gen_csv(self, fname: str = 'inv.csv') -> bool:
        try:
            with open(fname, 'w', newline='', encoding='utf-8') as f:
                w = csv.writer(f)
                w.writerow(['ID', 'UUID', 'Name', 'Resp', 'Qty', 'Consumable', 'Status', 'Img', 'Insts'])
                for t in self.get_tools():
                    insts = len(self.get_insts(t.id)) if not t.is_consumable else 0
                    w.writerow([
                        t.id,
                        t.tool_uuid,
                        t.name,
                        t.resp,
                        t.qty,
                        'Yes' if t.is_consumable else 'No',
                        t.status,
                        t.img or '',
                        insts
                    ])
            return True
        except IOError as e:
            logger.error("CSV err: %s", e)
            return False

def main(page: ft.Page):
    app = InvApp()
    page.title = "Inv Crisoull v2.3"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.window.width = 900
    page.window.height = 800
    page.scroll = ft.ScrollMode.AUTO
    selected_tools = {}  # Diccionario para rastrear herramientas seleccionadas
    current_user_role = None  # To store the logged-in user's role

    def launch_dashboard():
        def run_dashboard():
            from dashboard import dashboard_app
            ft.app(target=dashboard_app, port=0)  # Use port=0 to let Flet assign a free port
        threading.Thread(target=run_dashboard, daemon=True).start()

    def confirm(msg="¿Sure?"):
        def deco(func):
            @wraps(func)
            def wrap(*args, **kwargs):
                def yes(e):
                    dlg.open = False
                    page.update()
                    func(*args, **kwargs)
                def no(e):
                    dlg.open = False
                    page.update()
                dlg = ft.AlertDialog(
                    title=ft.Text("Confirm"),
                    content=ft.Text(msg),
                    actions=[
                        ft.TextButton("Yes", on_click=yes),
                        ft.TextButton("No", on_click=no)
                    ]
                )
                page.overlay.append(dlg)
                dlg.open = True
                page.update()
            return wrap
        return deco

    def toast(msg, clr=ft.colors.GREEN, dur=3000):
        sb = ft.SnackBar(
            ft.Text(msg),
            bgcolor=clr,
            duration=dur,
            action="Close",
            action_color=ft.colors.WHITE,
            elevation=10
        )
        page.overlay.append(sb)
        sb.open = True
        page.update()

    # Login UI
    username_inp = ft.TextField(label="Username", prefix_icon=icons.PERSON)
    password_inp = ft.TextField(label="Password", password=True, prefix_icon=icons.LOCK, can_reveal_password=True)
    login_btn = ft.ElevatedButton("Login", on_click=lambda e: login())

    def login():
        nonlocal current_user_role
        username = username_inp.value.strip()
        password = password_inp.value.strip()
        if username in USERS and USERS[username]["password"] == password:
            current_user_role = USERS[username]["role"]
            toast(f"Welcome, {username} ({current_user_role})")
            show_main_ui()
        else:
            toast("Invalid username or password", ft.colors.RED_400)

    def show_main_ui():
        # Clear the login UI
        page.controls.clear()
        page.update()

        # Define img_sel at the start of the scope
        img_sel = None  # To store the selected image path

        # Main UI components
        tools_row = ft.Row(wrap=True, scroll="auto", expand=True)
        n_inp = ft.TextField(label="Name", expand=1, prefix_icon=icons.INVENTORY)
        r_inp = ft.TextField(label="Resp", expand=1, prefix_icon=icons.PERSON)
        q_inp = ft.TextField(label="Qty", expand=1, prefix_icon=icons.NUMBERS, keyboard_type=ft.KeyboardType.NUMBER)
        c_inp = ft.Switch(label="Consumable", value=False)
        img_inp = ft.FilePicker(on_result=lambda e: add_img(e))
        s_inp = ft.TextField(label="Search", expand=1, prefix_icon=icons.SEARCH)
        loan_txt = ft.Text(size=20)
        tot_txt = ft.Text(size=20)
        stat_txt = ft.Text(value="Stats...", size=14, font_family="Roboto Mono")
        hist_cont = ft.ListView(expand=True, spacing=5, padding=10)

        # Disable inputs for worker role
        if current_user_role == "worker":
            n_inp.disabled = True
            r_inp.disabled = True
            q_inp.disabled = True
            c_inp.disabled = True

        def add_img(e):
            nonlocal img_sel
            if current_user_role == "worker":
                toast("Workers cannot upload images", ft.colors.RED_400)
                return
            img_sel = e.files[0].path if e.files else None
            toast(f"Img: {os.path.basename(img_sel)}" if img_sel else "No img")

        def upd_tools(filt=None):
            tools_row.controls.clear()
            selected_tools.clear()  # Reset selection
            try:
                tools = app.get_tools()
                if filt:
                    tools = [t for t in tools if filt.lower() in t.name.lower()]
                if not tools:
                    tools_row.controls.append(ft.Text("No tools", italic=True))
                for t in tools:
                    img_path = t.img if t.img and os.path.exists(t.img) else None
                    img_w = ft.Image(
                        src=img_path,
                        width=50,
                        height=50,
                        fit=ft.ImageFit.CONTAIN
                    ) if img_path else ft.Icon(icons.IMAGE_NOT_SUPPORTED)
                    insts = app.get_insts(t.id) if not t.is_consumable else []
                    chk = ft.Checkbox(
                        value=False,
                        on_change=lambda e, t_id=t.id: toggle_select(t_id, e.control.value),
                        disabled=current_user_role == "worker"  # Workers can't select tools for bulk actions
                    )
                    tools_row.controls.append(
                        ft.Card(
                            content=ft.Container(
                                content=ft.Column([
                                    ft.Row([
                                        chk,
                                        img_w,
                                        ft.ListTile(
                                            title=ft.Text(
                                                f"{t.name} (ID: {t.id})",
                                                size=16,
                                                weight="bold"
                                            ),
                                            subtitle=ft.Text(
                                                f"Resp: {t.resp}\nQty: {t.qty}\nStatus: {t.status}\nType: {'Consumable' if t.is_consumable else 'Reusable'}\nInsts: {len(insts) if not t.is_consumable else 'N/A'}"
                                            )
                                        )
                                    ]),
                                    ft.Row([
                                        ft.IconButton(
                                            icons.VISIBILITY,
                                            on_click=lambda _, t=t: show_tool(t),
                                            tooltip="View"
                                        ),
                                        ft.IconButton(
                                            icons.EDIT,
                                            on_click=lambda _, t=t: edit_tool(t),
                                            tooltip="Edit",
                                            disabled=current_user_role == "worker"  # Workers can't edit
                                        ),
                                        ft.IconButton(
                                            icons.DELETE,
                                            on_click=lambda _, id=t.id: del_tool(id),
                                            tooltip="Del",
                                            disabled=current_user_role == "worker"  # Workers can't delete
                                        ),
                                        ft.IconButton(
                                            icons.SEND,
                                            on_click=lambda _, t=t: loan_dlg(t),
                                            tooltip="Loan",
                                            disabled=t.is_consumable
                                        ),
                                        ft.IconButton(
                                            icons.QR_CODE,
                                            on_click=lambda _, t=t: regen_qr(t),
                                            tooltip="QR",
                                            disabled=t.is_consumable or current_user_role == "worker"  # Workers can't regen QR
                                        ),
                                        ft.IconButton(
                                            icons.UNDO,
                                            on_click=lambda _, t=t: ret_dlg(t),
                                            tooltip="Ret",
                                            disabled=t.is_consumable
                                        ),
                                        ft.IconButton(
                                            icons.REMOVE_CIRCLE,
                                            on_click=lambda _, t=t: consume_dlg(t),
                                            tooltip="Consume",
                                            disabled=not t.is_consumable or current_user_role == "worker"  # Workers can't consume
                                        )
                                    ], alignment=ft.MainAxisAlignment.END)
                                ]),
                                width=300,
                                padding=10
                            )
                        )
                    )
                page.update()
            except Exception as e:
                toast(f"List err: {str(e)}", ft.colors.RED_400)

        def toggle_select(tool_id: int, selected: bool):
            if current_user_role == "worker":
                toast("Workers cannot perform bulk actions", ft.colors.RED_400)
                return
            if selected:
                selected_tools[tool_id] = True
            else:
                selected_tools.pop(tool_id, None)

        def bulk_action(e, action: str):
            if current_user_role == "worker":
                toast("Workers cannot perform bulk actions", ft.colors.RED_400)
                return
            if not selected_tools:
                toast("No tools selected", ft.colors.RED_400)
                return
            if action == "delete":
                for tool_id in list(selected_tools.keys()):
                    ok, msg = app.del_tool(tool_id)
                    if ok:
                        selected_tools.pop(tool_id, None)
                        toast(msg)
                    else:
                        toast(msg, ft.colors.RED_400)
            upd_tools()
            upd_loans()
            calc_tot()

        def add_tool(e):
            nonlocal img_sel
            if current_user_role == "worker":
                toast("Workers cannot add tools", ft.colors.RED_400)
                return
            try:
                n, r, q = n_inp.value.strip(), r_inp.value.strip(), int(q_inp.value)
                is_consumable = c_inp.value
                if not n or not r:
                    return toast("Name/resp req", ft.colors.RED_400)
                if q < 0:
                    return toast("Qty >= 0", ft.colors.RED_400)
                ok, msg = app.add_tool(n, r, q, is_consumable, img_sel)
                if ok:
                    upd_tools()
                    n_inp.value = r_inp.value = q_inp.value = ""
                    c_inp.value = False
                    img_sel = None  # Reset after adding
                    upd_loans()
                    calc_tot()
                    toast(msg)
                else:
                    toast(msg, ft.colors.RED_400)
            except ValueError:
                toast("Invalid qty", ft.colors.RED_400)
            except Exception as e:
                toast(f"Err: {str(e)}", ft.colors.RED_400)

        def consume_dlg(t: Tool):
            if current_user_role == "worker":
                toast("Workers cannot consume tools", ft.colors.RED_400)
                return
            q_inp = ft.TextField(label="Qty to consume", keyboard_type=ft.KeyboardType.NUMBER)
            def reg(e):
                try:
                    qty = int(q_inp.value)
                    ok, msg = app.consume_tool(t.id, qty)
                    if ok:
                        upd_tools()
                        toast(msg)
                        dlg.open = False
                        page.update()
                    else:
                        toast(msg, ft.colors.RED_400)
                except ValueError:
                    toast("Invalid qty", ft.colors.RED_400)
            dlg = ft.AlertDialog(
                title=ft.Text(f"Consume: {t.name}"),
                content=ft.Column([q_inp]),
                actions=[
                    ft.TextButton("Consume", on_click=reg),
                    ft.TextButton("Cancel", on_click=lambda _: setattr(dlg, 'open', False))
                ]
            )
            page.overlay.append(dlg)
            dlg.open = True
            page.update()

        def edit_tool(t: Tool):
            if current_user_role == "worker":
                toast("Workers cannot edit tools", ft.colors.RED_400)
                return
            n_ed = ft.TextField(value=t.name, label="Name")
            r_ed = ft.TextField(value=t.resp, label="Resp")
            q_ed = ft.TextField(value=str(t.qty), label="Qty", keyboard_type=ft.KeyboardType.NUMBER)
            c_ed = ft.Switch(label="Consumable", value=t.is_consumable)
            img_ed = ft.FilePicker(on_result=lambda e: ed_img(e))
            img_sel_ed = None  # To store the edited image path
            img_curr = ft.Image(
                src=t.img,
                width=100,
                height=100,
                fit=ft.ImageFit.CONTAIN
            ) if t.img and os.path.exists(t.img) else ft.Text("No img")
            def ed_img(e):
                nonlocal img_sel_ed
                img_sel_ed = e.files[0].path if e.files else None
                if img_sel_ed:
                    img_curr.src = img_sel_ed
                page.update()
            def save(e):
                try:
                    n, r, q = n_ed.value.strip(), r_ed.value.strip(), int(q_ed.value)
                    is_consumable = c_ed.value
                    if not n or not r:
                        return toast("Name/resp req", ft.colors.RED_400)
                    if q < 0:
                        return toast("Qty >= 0", ft.colors.RED_400)
                    ok, msg = app.upd_tool(t.id, n, r, q, is_consumable, img_sel_ed or t.img)
                    if ok:
                        upd_tools()
                        upd_loans()
                        calc_tot()
                        toast(msg)
                        dlg.open = False
                    else:
                        toast(msg, ft.colors.RED_400)
                    page.update()
                except ValueError:
                    toast("Invalid qty", ft.colors.RED_400)
                except Exception as e:
                    toast(f"Err: {str(e)}", ft.colors.RED_400)
            dlg = ft.AlertDialog(
                title=ft.Text(f"Edit Tool (ID: {t.id})"),
                content=ft.Column([
                    n_ed,
                    r_ed,
                    q_ed,
                    c_ed,
                    ft.Row([ft.Text("Img:"), img_curr]),
                    ft.ElevatedButton(
                        "Change Img",
                        on_click=lambda e: img_ed.pick_files(allowed_extensions=["jpg", "png", "jpeg"])
                    )
                ]),
                actions=[
                    ft.TextButton("Save", on_click=save),
                    ft.TextButton("Cancel", on_click=lambda _: setattr(dlg, 'open', False))
                ]
            )
            page.overlay.extend([img_ed, dlg])
            dlg.open = True
            page.update()

        @confirm("Delete tool?")
        def del_tool(id: int):
            if current_user_role == "worker":
                toast("Workers cannot delete tools", ft.colors.RED_400)
                return
            ok, msg = app.del_tool(id)
            if ok:
                upd_tools()
                upd_loans()
                calc_tot()
                toast(msg)
            else:
                toast(msg, ft.colors.RED_400)

        def show_tool(t: Tool):
            insts = app.get_insts(t.id) if not t.is_consumable else []
            img_w = ft.Image(
                src=t.img,
                width=100,
                height=100,
                fit=ft.ImageFit.CONTAIN
            ) if t.img and os.path.exists(t.img) else ft.Text("No img")
            def qr_b64(qr_path: str) -> Optional[str]:
                try:
                    with open(qr_path, "rb") as f:
                        return base64.b64encode(f.read()).decode('utf-8')
                except Exception as e:
                    logger.error("QR b64 err: %s", e)
                    return None
            def dl_qr(e, i_id: int):
                if current_user_role == "worker":
                    toast("Workers cannot download QR codes", ft.colors.RED_400)
                    return
                qr_path = app.qr_mgr.gen_qr(t.tool_uuid, i_id, t.name)
                if qr_path and os.path.exists(qr_path):
                    dl_dir = os.path.expanduser("~/Downloads")
                    dest = os.path.join(dl_dir, os.path.basename(qr_path))
                    shutil.copy(qr_path, dest)
                    toast(f"QR saved: {dest}")
                else:
                    toast("QR dl err", ft.colors.RED_400)
            inst_btns = ft.Column([
                ft.Row([
                    ft.Text(f"{i.serial} ({i.status})"),
                    ft.Image(
                        src_base64=qr_b64(app.qr_mgr.gen_qr(t.tool_uuid, i.id, t.name)),
                        width=80,
                        height=80,
                        fit=ft.ImageFit.CONTAIN
                    ) if app.qr_mgr.gen_qr(t.tool_uuid, i.id, t.name) else ft.Text("No QR"),
                    ft.IconButton(
                        icons.DOWNLOAD,
                        on_click=lambda e, i_id=i.id: dl_qr(e, i_id),
                        tooltip="DL QR",
                        disabled=current_user_role == "worker"
                    )
                ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
                for i in insts
            ], scroll=ft.ScrollMode.AUTO)
            dlg = ft.AlertDialog(
                title=ft.Text(f"{t.name} Details"),
                content=ft.Column([
                    img_w,
                    ft.Text(f"ID: {t.id}"),
                    ft.Text(f"UUID: {t.tool_uuid}"),
                    ft.Text(f"Name: {t.name}"),
                    ft.Text(f"Resp: {t.resp}"),
                    ft.Text(f"Qty: {t.qty}"),
                    ft.Text(f"Type: {'Consumable' if t.is_consumable else 'Reusable'}"),
                    ft.Text(f"Status: {t.status}"),
                    ft.Text("Insts:" if not t.is_consumable else "No insts (consumable)"),
                    inst_btns
                ], scroll=ft.ScrollMode.AUTO),
                actions=[
                    ft.TextButton("Close", on_click=lambda _: setattr(dlg, 'open', False))
                ]
            )
            page.overlay.append(dlg)
            dlg.open = True
            page.update()

        def loan_dlg(t: Tool):
            w_inp = ft.TextField(label="Worker")
            i_dd = ft.Dropdown(label="Inst")
            insts = app.get_insts(t.id)
            i_dd.options = [
                ft.dropdown.Option(key=str(i.id), text=f"{i.serial} ({i.status})")
                for i in insts if i.status == "avail"
            ]
            def reg(e):
                try:
                    w, i_id = w_inp.value.strip(), i_dd.value
                    if not w or not i_id:
                        return toast("Worker/inst req", ft.colors.RED_400)
                    if app.reg_loan(t.id, int(i_id), w):
                        upd_tools()
                        upd_loans()
                        toast(f"Loaned: {t.name}")
                        dlg.open = False
                        page.update()
                    else:
                        toast("Loan err", ft.colors.RED_400)
                except ValueError:
                    toast("Invalid inst", ft.colors.RED_400)
            dlg = ft.AlertDialog(
                title=ft.Text(f"Loan: {t.name}"),
                content=ft.Column([w_inp, i_dd]),
                actions=[
                    ft.TextButton("Reg", on_click=reg),
                    ft.TextButton("Cancel", on_click=lambda _: setattr(dlg, 'open', False))
                ]
            )
            page.overlay.append(dlg)
            dlg.open = True
            page.update()

        def ret_dlg(t: Tool):
            w_inp = ft.TextField(label="Worker")
            i_dd = ft.Dropdown(label="Inst")
            n_inp = ft.TextField(label="Notes (opt)", multiline=True)
            insts = app.get_insts(t.id)
            i_dd.options = [
                ft.dropdown.Option(key=str(i.id), text=f"{i.serial} ({i.status})")
                for i in insts if i.status == "loaned"
            ]
            def reg(e):
                try:
                    w, i_id, n = w_inp.value.strip(), i_dd.value, n_inp.value.strip()
                    if not w or not i_id:
                        return toast("Worker/inst req", ft.colors.RED_400)
                    ret = RetData(h_id=t.id, i_id=int(i_id), worker=w, notes=n)
                    if app.qr_mgr.reg_ret(ret):
                        upd_tools()
                        upd_loans()
                        toast(f"Returned: {t.name}")
                        dlg.open = False
                        page.update()
                    else:
                        toast("Ret err", ft.colors.RED_400)
                except ValueError:
                    toast("Invalid inst", ft.colors.RED_400)
            dlg = ft.AlertDialog(
                title=ft.Text(f"Return: {t.name}"),
                content=ft.Column([w_inp, i_dd, n_inp]),
                actions=[
                    ft.TextButton("Reg", on_click=reg),
                    ft.TextButton("Cancel", on_click=lambda _: setattr(dlg, 'open', False))
                ]
            )
            page.overlay.append(dlg)
            dlg.open = True
            page.update()

        def regen_qr(t: Tool):
            if current_user_role == "worker":
                toast("Workers cannot regenerate QR codes", ft.colors.RED_400)
                return
            i_dd = ft.Dropdown(label="Inst")
            insts = app.get_insts(t.id)
            i_dd.options = [
                ft.dropdown.Option(key=str(i.id), text=f"{i.serial} ({i.status})")
                for i in insts
            ]
            def reg(e):
                try:
                    i_id = i_dd.value
                    if not i_id:
                        return toast("Inst req", ft.colors.RED_400)
                    if app.regen_qr(t.tool_uuid, int(i_id), t.name):
                        toast(f"QR regen: {t.name}")
                        dlg.open = False
                        page.update()
                    else:
                        toast("QR err", ft.colors.RED_400)
                except ValueError:
                    toast("Invalid inst", ft.colors.RED_400)
            dlg = ft.AlertDialog(
                title=ft.Text(f"Regen QR: {t.name}"),
                content=ft.Column([i_dd]),
                actions=[
                    ft.TextButton("Reg", on_click=reg),
                    ft.TextButton("Cancel", on_click=lambda _: setattr(dlg, 'open', False))
                ]
            )
            page.overlay.append(dlg)
            dlg.open = True
            page.update()

        def upd_loans():
            try:
                loan_txt.value = f"Overdue: {len(app.check_overdue())}"
                page.update()
            except Exception as e:
                toast(f"Loans err: {str(e)}", ft.colors.RED_400)

        def calc_tot():
            try:
                tot = sum(t.qty for t in app.get_tools())
                tot_txt.value = f"Total: {tot}"
                page.update()
            except Exception as e:
                toast(f"Tot err: {str(e)}", ft.colors.RED_400)

        def upd_stats():
            try:
                s = app.qr_mgr.get_stats()
                stat_txt.value = (
                    f"Loaned: {s['loaned']}\n"
                    f"Loans Today: {s['loans_today']}\n"
                    f"Rets Today: {s['rets_today']}\n"
                    f"Pop:\n" + "\n".join(f" - {t['name']}: {t['loans']}" for t in s['pop_tools']) +
                    f"\nUpdated: {s['ts']}"
                )
                page.update()
            except Exception as e:
                toast(f"Stats err: {str(e)}", ft.colors.RED_400)

        def upd_hist():
            try:
                app.c.execute('''
                    SELECT d.id, h.name, ti.serial, d.worker, d.date, d.notes
                    FROM rets d
                    JOIN tools h ON d.h_id = h.id
                    JOIN tool_inst ti ON d.i_id = ti.id
                    ORDER BY d.date DESC
                ''')
                hist_cont.controls = [
                    ft.ListTile(
                        leading=ft.Icon(icons.RECEIPT),
                        title=ft.Text(f"{v[1]} - {v[2]}", weight="bold"),
                        subtitle=ft.Text(f"Worker: {v[3]}\nDate: {v[4]}\nNotes: {v[5] or 'N/A'}"),
                        trailing=ft.Icon(icons.CHECK_CIRCLE, color=ft.colors.GREEN)
                    ) for v in app.c.fetchall()
                ]
                page.update()
            except Exception as e:
                toast(f"Hist err: {str(e)}", ft.colors.RED_400)

        def gen_csv():
            if current_user_role == "worker":
                toast("Workers cannot generate CSV reports", ft.colors.RED_400)
                return
            try:
                if app.gen_csv():
                    toast("CSV OK")
                else:
                    toast("CSV err", ft.colors.RED_400)
            except Exception as e:
                toast(f"CSV err: {str(e)}", ft.colors.RED_400)

        def exp_qrs():
            if current_user_role == "worker":
                toast("Workers cannot export QR codes", ft.colors.RED_400)
                return
            try:
                tools = app.get_tools()
                zip_path = os.path.expanduser("~/Downloads/qrs.zip")
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
                    for t in tools:
                        if t.is_consumable:
                            continue
                        for i in app.get_insts(t.id):
                            qr = app.qr_mgr.gen_qr(t.tool_uuid, i.id, t.name)
                            if qr and os.path.exists(qr):
                                z.write(qr, os.path.basename(qr))
                toast(f"QRs: {zip_path}")
            except Exception as e:
                toast(f"QRs err: {str(e)}", ft.colors.RED_400)

        def toggle_menu(e):
            page.drawer.open = not page.drawer.open
            page.update()

        def chg_theme(e):
            page.theme_mode = ft.ThemeMode.DARK if page.theme_mode == ft.ThemeMode.LIGHT else ft.ThemeMode.LIGHT
            theme_ic.icon = icons.DARK_MODE if page.theme_mode == ft.ThemeMode.LIGHT else icons.LIGHT_MODE
            page.update()

        def exit_app(e):
            page.window.close()

        menu_btn = ft.IconButton(icon=icons.MENU, on_click=toggle_menu, tooltip="Menu")
        theme_ic = ft.IconButton(icon=icons.BRIGHTNESS_6, on_click=chg_theme, tooltip="Theme")
        page.drawer = ft.NavigationDrawer(
            bgcolor=ft.colors.SURFACE,
            elevation=16,
            controls=[
                ft.Container(
                    padding=ft.padding.all(20),
                    content=ft.Column([
                        ft.Row([
                            ft.Icon(icons.DASHBOARD, size=30),
                            ft.Text("Control", style=ft.TextThemeStyle.HEADLINE_SMALL)
                        ]),
                        ft.Divider(height=20),
                        ft.ExpansionTile(
                            title=ft.Text("Hist"),
                            leading=ft.Icon(icons.HISTORY),
                            trailing=ft.IconButton(icon=icons.REFRESH, on_click=lambda e: upd_hist()),
                            maintain_state=True,
                            controls=[
                                ft.Container(
                                    content=hist_cont,
                                    height=300,
                                    border=ft.border.all(1, ft.colors.GREY_300),
                                    border_radius=10
                                )
                            ]
                        ),
                        ft.ExpansionTile(
                            title=ft.Text("Stats"),
                            leading=ft.Icon(icons.ANALYTICS),
                            trailing=ft.IconButton(icon=icons.REFRESH, on_click=lambda e: upd_stats()),
                            maintain_state=True,
                            controls=[
                                ft.Container(
                                    content=stat_txt,
                                    padding=10,
                                    border=ft.border.all(1, ft.colors.GREY_300),
                                    border_radius=10
                                )
                            ]
                        ),
                        ft.Divider(height=20),
                        ft.Text("Actions", style=ft.TextThemeStyle.TITLE_MEDIUM),
                        ft.Column([
                            ft.ElevatedButton(
                                "CSV",
                                icon=icons.DOWNLOAD,
                                on_click=lambda e: gen_csv(),
                                style=ft.ButtonStyle(
                                    shape=ft.RoundedRectangleBorder(radius=8),
                                    bgcolor=ft.colors.BLUE_600,
                                    color=ft.colors.WHITE
                                ),
                                width=200,
                                disabled=current_user_role == "worker"
                            ),
                            ft.ElevatedButton(
                                "QRs",
                                icon=icons.QR_CODE,
                                on_click=lambda e: exp_qrs(),
                                style=ft.ButtonStyle(
                                    shape=ft.RoundedRectangleBorder(radius=8),
                                    bgcolor=ft.colors.PURPLE_600,
                                    color=ft.colors.WHITE
                                ),
                                width=200,
                                disabled=current_user_role == "worker"
                            ),
                            ft.ElevatedButton(
                                "Delete Selected",
                                icon=icons.DELETE,
                                on_click=lambda e: bulk_action(e, "delete"),
                                style=ft.ButtonStyle(
                                    shape=ft.RoundedRectangleBorder(radius=8),
                                    bgcolor=ft.colors.RED_600,
                                    color=ft.colors.WHITE
                                ),
                                width=200,
                                disabled=current_user_role == "worker"
                            ),
                            ft.ElevatedButton(
                                "Exit",
                                icon=icons.EXIT_TO_APP,
                                on_click=exit_app,
                                style=ft.ButtonStyle(
                                    shape=ft.RoundedRectangleBorder(radius=8),
                                    bgcolor=ft.colors.GREY_600,
                                    color=ft.colors.WHITE
                                ),
                                width=200
                            )
                        ], spacing=10)
                    ], scroll=ft.ScrollMode.AUTO)
                )
            ]
        )

        page.add(
            ft.AppBar(
                leading=menu_btn,
                title=ft.Text("Inv Crisoull v2.3"),
                actions=[
                    ft.IconButton(icons.DOWNLOAD, on_click=lambda e: gen_csv(), tooltip="CSV", disabled=current_user_role == "worker"),
                    ft.IconButton(
                        icons.ANALYTICS,
                        on_click=lambda e: launch_dashboard() if current_user_role == "admin" else toast("Admins only", ft.colors.RED_400),
                        tooltip="Analytics Dashboard",
                        disabled=current_user_role != "admin"
                    ),
                    theme_ic,
                    ft.IconButton(icons.EXIT_TO_APP, on_click=exit_app, tooltip="Exit")
                ],
                bgcolor=ft.colors.SURFACE_VARIANT,
                elevation=4
            ),
            ft.Row([
                ft.Column([
                    ft.Row([
                        n_inp,
                        r_inp,
                        q_inp,
                        c_inp,
                        ft.ElevatedButton(
                            "Img",
                            icon=icons.UPLOAD,
                            on_click=lambda e: img_inp.pick_files(allowed_extensions=["jpg", "png", "jpeg"]),
                            disabled=current_user_role == "worker"
                        ),
                        ft.ElevatedButton(
                            "Add",
                            on_click=add_tool,
                            icon=icons.ADD,
                            style=ft.ButtonStyle(bgcolor=ft.colors.BLUE_600, color=ft.colors.WHITE),
                            disabled=current_user_role == "worker"
                        )
                    ]),
                    ft.Divider(),
                    ft.Row([
                        s_inp,
                        ft.ElevatedButton(
                            "Search",
                            on_click=lambda e: upd_tools(s_inp.value),
                            icon=icons.SEARCH
                        )
                    ]),
                    ft.Divider(),
                    tools_row,
                    ft.Divider(),
                    ft.Row([loan_txt, tot_txt], alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
                ], expand=True, scroll=ft.ScrollMode.AUTO)
            ], expand=True)
        )
        page.overlay.append(img_inp)
        upd_tools()
        upd_loans()
        calc_tot()
        upd_hist()
        upd_stats()

    # Show login UI initially
    page.add(
        ft.Column([
            ft.Text("Login to Inv Crisoull", size=24, weight="bold"),
            username_inp,
            password_inp,
            login_btn
        ], alignment=ft.MainAxisAlignment.CENTER, horizontal_alignment=ft.CrossAxisAlignment.CENTER)
    )

ft.app(target=main)