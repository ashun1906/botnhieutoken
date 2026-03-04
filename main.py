import logging
import os
import pytz
import sqlite3
import json
import asyncio
import sys
import threading
import queue as _queue
import time
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update, MessageEntity
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes, ConversationHandler, ChatMemberHandler
)
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from telegram.error import RetryAfter, Forbidden, BadRequest, NetworkError, TimedOut
from http.server import BaseHTTPRequestHandler, HTTPServer

# --- CẤU HÌNH ---
def _env_tokens():
    s = os.getenv("TELEGRAM_TOKENS", "")
    single = os.getenv("TELEGRAM_TOKEN", "")
    items = [x.strip() for x in s.split(",") if x.strip()]
    if not items and single.strip():
        items = [single.strip()]
    return items

TOKENS = _env_tokens()
if not TOKENS:
    logging.error("Thiếu TELEGRAM_TOKENS hoặc TELEGRAM_TOKEN")
    sys.exit(1)

_owner = os.getenv("OWNER_ID") or os.getenv("TELEGRAM_OWNER_ID")
if not _owner or not _owner.isdigit():
    logging.error("Thiếu OWNER_ID")
    sys.exit(1)
OWNER_ID = int(_owner)


# --- DATABASE ---
def db_execute(query, params=(), fetch=False):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    try:
        c.execute(query, params)
        res = c.fetchall() if fetch else None
        conn.commit()
        return res
    except Exception as e:
        logging.error(f"❌ DB Error: {e}")
        return []
    finally:
        conn.close()

def init_db():
    db_execute('''CREATE TABLE IF NOT EXISTS groups (id INTEGER PRIMARY KEY, title TEXT, active INTEGER DEFAULT 1)''')
    db_execute('''CREATE TABLE IF NOT EXISTS admins (id INTEGER PRIMARY KEY, name TEXT)''')
    db_execute('''CREATE TABLE IF NOT EXISTS posts (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)''')
    db_execute('''CREATE TABLE IF NOT EXISTS schedules (time TEXT, post_id INTEGER, PRIMARY KEY (time, post_id))''')
    db_execute('INSERT OR IGNORE INTO admins VALUES (?, ?)', (OWNER_ID, "Chủ Nhân"))

init_db()
logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
scheduler = BackgroundScheduler(
    executors={'default': ThreadPoolExecutor(1)},
    job_defaults={'misfire_grace_time': 300, 'max_instances': 1},
    timezone=pytz.timezone(os.getenv("TZ", 'Asia/Ho_Chi_Minh'))
)

# --- STATES ---
WAIT_POST, WAIT_SCHED, WAIT_EDIT_POST, WAIT_ADD_ADMIN = range(1, 5)

# --- HELPER FUNCTIONS ---
def get_main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 Quản lý Bài Viết", callback_data='menu_post')],
        [InlineKeyboardButton("📅 Lên Lịch & Đăng bài", callback_data='menu_sched')],
        [InlineKeyboardButton("⚙️ Nhóm & Admin", callback_data='menu_management')],
    ])

async def send_broadcast(bot, post_id, admin_id=None):
    res = db_execute("SELECT data FROM posts WHERE id = ?", (post_id,), fetch=True)
    if not res: return
    p_data = json.loads(res[0][0])
    ent = [MessageEntity(**en) for en in p_data["entities"]] if p_data.get("entities") else None
    groups = db_execute("SELECT id FROM groups WHERE active = 1", fetch=True)
    success, fail = 0, 0
    for gid in [g[0] for g in groups]:
        attempts = 0
        while attempts < 5:
            try:
                if p_data.get("copy"):
                    sent = False
                    try:
                        src = p_data["copy"]
                        await bot.copy_message(chat_id=gid, from_chat_id=src["chat_id"], message_id=src["message_id"])
                        sent = True
                    except RetryAfter as e:
                        raise e
                    except Exception as e:
                        try:
                            logging.warning(f"copy_message failed gid={gid}: {e}")
                        except:
                            pass
                        sent = False
                    if not sent:
                        if p_data["type"] == "text":
                            await bot.send_message(gid, p_data["text"], entities=ent)
                        elif p_data["type"] == "photo":
                            await bot.send_photo(gid, p_data["media"], caption=p_data["text"], caption_entities=ent)
                        elif p_data.get("type") == "video":
                            await bot.send_video(gid, p_data["media"], caption=p_data["text"], caption_entities=ent)
                else:
                    if p_data["type"] == "text":
                        await bot.send_message(gid, p_data["text"], entities=ent)
                    elif p_data["type"] == "photo":
                        await bot.send_photo(gid, p_data["media"], caption=p_data["text"], caption_entities=ent)
                    elif p_data.get("type") == "video":
                        await bot.send_video(gid, p_data["media"], caption=p_data["text"], caption_entities=ent)
                success += 1
                await asyncio.sleep(1.0)
                break
            except RetryAfter as e:
                try:
                    logging.warning(f"RetryAfter gid={gid}: retry_after={getattr(e,'retry_after',None)}")
                except:
                    pass
                await asyncio.sleep(getattr(e, "retry_after", 1) + 1.0)
                attempts += 1
                if attempts >= 5:
                    fail += 1
            except Forbidden as e:
                try:
                    logging.warning(f"Forbidden gid={gid}: {e}")
                except:
                    pass
                try:
                    db_execute("UPDATE groups SET active = 0 WHERE id = ?", (gid,))
                except:
                    pass
                fail += 1
                break
            except BadRequest as e:
                attempts += 1
                try:
                    logging.warning(f"BadRequest gid={gid}: {e}, attempt={attempts}")
                except:
                    pass
                await asyncio.sleep(0.8 * attempts)
                if attempts >= 5:
                    fail += 1
            except (NetworkError, TimedOut) as e:
                attempts += 1
                try:
                    logging.warning(f"Network issue gid={gid}: {e}, attempt={attempts}")
                except:
                    pass
                await asyncio.sleep(1.0 * attempts)
                if attempts >= 5:
                    fail += 1
            except Exception as e:
                attempts += 1
                try:
                    logging.warning(f"Unexpected send error gid={gid}: {e}, attempt={attempts}")
                except:
                    pass
                if p_data.get("copy"):
                    try:
                        if p_data["type"] == "text":
                            await bot.send_message(gid, p_data["text"], entities=ent)
                        elif p_data["type"] == "photo":
                            await bot.send_photo(gid, p_data["media"], caption=p_data["text"], caption_entities=ent)
                        elif p_data.get("type") == "video":
                            await bot.send_video(gid, p_data["media"], caption=p_data["text"], caption_entities=ent)
                        success += 1
                        await asyncio.sleep(1.0)
                        break
                    except Exception:
                        pass
                await asyncio.sleep(1.0 * attempts)
                if attempts >= 5:
                    fail += 1
    report = f"📊 Báo cáo bài #{post_id}: Thành công {success}, Thất bại {fail}"
    try: await bot.send_message(admin_id or OWNER_ID, report)
    except: pass

_post_queue = _queue.Queue()
_worker_thread = None
_bot_loops = {}
def _queue_worker():
    while True:
        item = _post_queue.get()
        try:
            if isinstance(item, tuple) and len(item) == 3:
                bot, post_id, admin_id = item
            elif isinstance(item, tuple) and len(item) == 2:
                bot, post_id = item
                admin_id = OWNER_ID
            else:
                _post_queue.task_done()
                continue
            loop = _bot_loops.get(id(bot))
            if loop and loop.is_running():
                fut = asyncio.run_coroutine_threadsafe(send_broadcast(bot, post_id, admin_id), loop)
                try:
                    fut.result()
                except Exception as e:
                    try:
                        logging.error(f"Broadcast future error: {e}")
                    except:
                        pass
            else:
                try:
                    logging.warning("Bot loop not found or not running; skipping broadcast")
                except:
                    pass
        except Exception as e:
            try:
                logging.error(f"Queue worker error: {e}")
            except:
                pass
        finally:
            _post_queue.task_done()
            time.sleep(0.5)

def run_async_job(bot, post_id):
    try:
        _post_queue.put((bot, post_id, OWNER_ID))
    except:
        try:
            loop = _bot_loops.get(id(bot))
            if loop and loop.is_running():
                fut = asyncio.run_coroutine_threadsafe(send_broadcast(bot, post_id, OWNER_ID), loop)
                fut.result()
        except:
            pass

def _ensure_worker():
    global _worker_thread
    try:
        if _worker_thread is None or not _worker_thread.is_alive():
            _worker_thread = threading.Thread(target=_queue_worker, daemon=True)
            _worker_thread.start()
    except Exception as e:
        try:
            logging.error(f"Ensure worker failed: {e}")
        except:
            pass

def _watchdog():
    while True:
        try:
            _ensure_worker()
        except:
            pass
        time.sleep(3)

class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/healthz"):
            try:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
            except:
                pass
        else:
            try:
                self.send_response(404)
                self.end_headers()
            except:
                pass
    def log_message(self, format, *args):
        return

def _start_http_server():
    try:
        port = int(os.getenv("PORT", "8000"))
        server = HTTPServer(("0.0.0.0", port), _HealthHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()
    except Exception as e:
        try:
            logging.error(f"HTTP server error: {e}")
        except:
            pass

# --- HANDLERS ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admins = [a[0] for a in db_execute("SELECT id FROM admins", fetch=True)]
    if update.effective_user.id not in admins: return
    await update.message.reply_text("🌟 HỆ THỐNG QUẢN TRỊ BOT TELEGRAM", reply_markup=get_main_menu())
    return ConversationHandler.END

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    user_id = update.effective_user.id
    try: await query.answer()
    except: pass

    if data == 'to_main':
        await query.edit_message_text("🌟 HỆ THỐNG QUẢN TRỊ V6.9 PRO", reply_markup=get_main_menu())
        return ConversationHandler.END
    
    elif data == 'menu_post':
        kb = [[InlineKeyboardButton("➕ Tạo Bài Mới", callback_data='create_post')],
              [InlineKeyboardButton("👀 Danh Sách (Sửa/Xóa)", callback_data='list_posts')],
              [InlineKeyboardButton("🏠 TRANG CHỦ", callback_data='to_main')]]
        await query.edit_message_text("📝 QUẢN LÝ BÀI VIẾT:", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END

    elif data == 'create_post':
        await query.edit_message_text("Gửi nội dung bài mới (Văn bản/Ảnh/Video):")
        return WAIT_POST

    elif data == 'list_posts':
        posts = db_execute("SELECT id FROM posts", fetch=True)
        if not posts: return await query.edit_message_text("❌ Trống.", reply_markup=get_main_menu())
        kb = [[InlineKeyboardButton(f"Bài viết #{p[0]}", callback_data=f"viewp_{p[0]}")] for p in posts]
        kb.append([InlineKeyboardButton("🔙", callback_data='menu_post')])
        await query.edit_message_text("Chọn bài:", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith('viewp_'):
        pid = data.split('_')[1]
        kb = [[InlineKeyboardButton(f"✏️ Sửa bài #{pid}", callback_data=f"editp_{pid}")],
              [InlineKeyboardButton(f"🗑 Xóa bài #{pid}", callback_data=f"delp_{pid}")],
              [InlineKeyboardButton("🔙", callback_data='list_posts')]]
        await query.message.reply_text(f"📌 Đang chọn bài #{pid}:", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith('editp_'):
        context.user_data['edit_pid'] = data.split('_')[1]
        await context.bot.send_message(user_id, f"🛠 Gửi nội dung MỚI cho bài #{context.user_data['edit_pid']}:")
        return WAIT_EDIT_POST

    elif data.startswith('delp_'):
        pid = data.split('_')[1]
        db_execute("DELETE FROM schedules WHERE post_id = ?", (pid,))
        db_execute("DELETE FROM posts WHERE id = ?", (pid,))
        await query.edit_message_text(f"✅ Đã xóa Bài #{pid}!")

    elif data == 'menu_management':
        kb = [[InlineKeyboardButton("📋 Nhóm", callback_data='show_groups')], [InlineKeyboardButton("👥 Admin", callback_data='menu_admin')], [InlineKeyboardButton("🏠", callback_data='to_main')]]
        await query.edit_message_text("⚙️ QUẢN LÝ:", reply_markup=InlineKeyboardMarkup(kb))

    elif data == 'show_groups':
        groups = db_execute("SELECT title, id FROM groups WHERE active = 1", fetch=True)
        text = "📋 **NHÓM:**\n" + ("\n".join([f"• {g[0]} (`{g[1]}`)" for g in groups]) if groups else "Trống")
        kb = [
            [InlineKeyboardButton("🗑 Xóa Nhóm", callback_data='del_group')],
            [InlineKeyboardButton("🔙", callback_data='menu_management')]
        ]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    elif data == 'del_group':
        groups = db_execute("SELECT id, title FROM groups WHERE active = 1", fetch=True)
        if not groups:
            kb = [[InlineKeyboardButton("🔙", callback_data='show_groups')]]
            await query.edit_message_text("❌ Không có nhóm nào để xóa.", reply_markup=InlineKeyboardMarkup(kb))
            return ConversationHandler.END
        kb = [[InlineKeyboardButton(f"🗑 {title} ({gid})", callback_data=f"delgroup_{gid}")] for gid, title in groups]
        kb.append([InlineKeyboardButton("🔙", callback_data='show_groups')])
        await query.edit_message_text("Chọn nhóm để xóa:", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith('delgroup_'):
        gid = int(data.split('_')[1])
        db_execute("DELETE FROM groups WHERE id = ?", (gid,))
        await query.edit_message_text(f"✅ Đã xóa Nhóm {gid}.")
        
        # Refresh group list
        groups = db_execute("SELECT id, title FROM groups WHERE active = 1", fetch=True)
        if not groups:
            kb = [[InlineKeyboardButton("🔙", callback_data='show_groups')]]
            await query.edit_message_text("Tất cả các nhóm đã bị xóa.", reply_markup=InlineKeyboardMarkup(kb))
            return ConversationHandler.END
        kb = [[InlineKeyboardButton(f"🗑 {title} ({g_id})", callback_data=f"delgroup_{g_id}")] for g_id, title in groups]
        kb.append([InlineKeyboardButton("🔙", callback_data='show_groups')])
        await query.message.reply_text("Chọn nhóm khác để xóa:", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END



    elif data == 'menu_admin':
        admins = db_execute("SELECT id, name FROM admins", fetch=True)
        text = "👥 **ADMINS:**\n" + "\n".join([f"• {a[1]} (`{a[0]}`)" for a in admins])
        kb = [[InlineKeyboardButton("➕ Thêm", callback_data='add_admin')], [InlineKeyboardButton("🗑 Xóa", callback_data='del_admin')], [InlineKeyboardButton("🔙", callback_data='menu_management')]]
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode='Markdown')

    elif data == 'add_admin':
        await query.edit_message_text("Nhập ID Admin mới:")
        return WAIT_ADD_ADMIN

    elif data == 'del_admin':
        admins = db_execute("SELECT id, name FROM admins", fetch=True)
        items = [(a[0], a[1]) for a in admins if a[0] != OWNER_ID]
        if not items:
            kb = [[InlineKeyboardButton("🔙", callback_data='menu_admin')]]
            await query.edit_message_text("❌ Không có admin nào để xóa.", reply_markup=InlineKeyboardMarkup(kb))
            return ConversationHandler.END
        kb = [[InlineKeyboardButton(f"🗑 {name} ({aid})", callback_data=f"deladm_{aid}")] for aid, name in items]
        kb.append([InlineKeyboardButton("🔙", callback_data='menu_admin')])
        await query.edit_message_text("Chọn admin để xóa:", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END

    elif data.startswith('deladm_'):
        aid = int(data.split('_')[1])
        if aid == OWNER_ID:
            await query.answer()
            return ConversationHandler.END
        exists = db_execute("SELECT 1 FROM admins WHERE id = ?", (aid,), fetch=True)
        if exists:
            db_execute("DELETE FROM admins WHERE id = ?", (aid,))
            await query.edit_message_text(f"✅ Đã xóa Admin {aid}.")
        else:
            await query.edit_message_text("❌ Không tìm thấy Admin này.")
        admins = db_execute("SELECT id, name FROM admins", fetch=True)
        items = [(a[0], a[1]) for a in admins if a[0] != OWNER_ID]
        kb = [[InlineKeyboardButton(f"🗑 {name} ({i})", callback_data=f"deladm_{i}")] for i, name in items]
        kb.append([InlineKeyboardButton("🔙", callback_data='menu_admin')])
        await query.message.reply_text("Chọn admin để xóa:", reply_markup=InlineKeyboardMarkup(kb))
        return ConversationHandler.END

    elif data == 'menu_sched':
        kb = [[InlineKeyboardButton("⏰ Đặt lịch", callback_data='pre_set_sched')],
              [InlineKeyboardButton("🚀 Đăng ngay", callback_data='pre_post_now')],
              [InlineKeyboardButton("📋 Xem lịch", callback_data='view_sched')],
              [InlineKeyboardButton("🗑 Xóa lịch theo bài", callback_data='clear_sched_by_post')],
              [InlineKeyboardButton("🗑 Xóa sạch lịch", callback_data='clear_sched')],
              [InlineKeyboardButton("🏠 TRANG CHỦ", callback_data='to_main')]]
        await query.edit_message_text("📅 LỊCH TRÌNH:", reply_markup=InlineKeyboardMarkup(kb))

    elif data == 'view_sched':
        jobs = db_execute("SELECT time, post_id FROM schedules", fetch=True)
        text = "📋 **LỊCH ĐANG CHẠY:**\n\n" + ("\n".join([f"⏰ `{j[0]}` -> Bài #{j[1]}" for j in sorted(jobs)]) if jobs else "❌ Trống.")
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data='menu_sched')]]), parse_mode='Markdown')

    elif data == 'clear_sched':
        db_execute("DELETE FROM schedules")
        for job in scheduler.get_jobs(): job.remove()
        await query.edit_message_text("✅ Đã xóa sạch lịch!", reply_markup=get_main_menu())

    elif data == 'clear_sched_by_post':
        posts = db_execute("SELECT id FROM posts", fetch=True)
        if not posts:
            await query.edit_message_text("❌ Chưa có bài viết.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙", callback_data='menu_sched')]]))
            return ConversationHandler.END
        kb = [[InlineKeyboardButton(f"Bài #{p[0]}", callback_data=f"clearsched_{p[0]}")] for p in posts]
        kb.append([InlineKeyboardButton("🔙", callback_data='menu_sched')])
        await query.edit_message_text("Chọn bài để xóa lịch:", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith('clearsched_'):
        pid = data.split('_')[1]
        times = db_execute("SELECT time FROM schedules WHERE post_id = ?", (pid,), fetch=True)
        for (t,) in times:
            try:
                scheduler.remove_job(f"{t}_{pid}")
            except:
                pass
        db_execute("DELETE FROM schedules WHERE post_id = ?", (pid,))
        await query.edit_message_text(f"✅ Đã xóa lịch của Bài #{pid}!", reply_markup=get_main_menu())

    elif data in ['pre_set_sched', 'pre_post_now']:
        posts = db_execute("SELECT id FROM posts", fetch=True)
        if not posts: return await query.edit_message_text("❌ Hãy tạo bài trước!", reply_markup=get_main_menu())
        m = "S" if data == 'pre_set_sched' else "N"
        kb = [[InlineKeyboardButton(f"Bài #{p[0]}", callback_data=f"selp_{m}_{p[0]}")] for p in posts]
        await query.edit_message_text("Chọn bài:", reply_markup=InlineKeyboardMarkup(kb))

    elif data.startswith('selp_'):
        _, mode, pid = data.split('_')
        if mode == "N": 
            await query.edit_message_text(f"🚀 Đã đưa bài #{pid} vào hàng chờ!")
            try:
                _post_queue.put((context.bot, pid, user_id))
            except:
                try:
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    loop.run_until_complete(send_broadcast(context.bot, pid, user_id))
                    loop.close()
                except:
                    pass
        else:
            context.user_data['temp_pid'] = pid
            await query.edit_message_text(f"Nhập giờ (HH:MM):")
            return WAIT_SCHED

# --- INPUT PROCESSING ---
async def save_post_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    is_edit = 'edit_pid' in context.user_data
    pid = context.user_data.get('edit_pid')
    
    raw = msg.entities or msg.caption_entities
    ent = [e.to_dict() for e in raw] if raw else []
    d = {"entities": ent, "text": msg.text or msg.caption, "type": "text", "media": msg.photo[-1].file_id if msg.photo else (msg.video.file_id if msg.video else None)}
    if msg.photo: d["type"] = "photo"
    elif msg.video: d["type"] = "video"
    d["copy"] = {"chat_id": msg.chat_id, "message_id": msg.message_id}

    if is_edit:
        db_execute("UPDATE posts SET data = ? WHERE id = ?", (json.dumps(d), pid))
        await msg.reply_text(f"✅ Đã sửa bài #{pid}!", reply_markup=get_main_menu())
        del context.user_data['edit_pid']
    else:
        db_execute("INSERT INTO posts (data) VALUES (?)", (json.dumps(d),))
        await msg.reply_text("✅ Đã tạo bài mới!", reply_markup=get_main_menu())
    
    return ConversationHandler.END

async def save_sched_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pid = context.user_data.get('temp_pid')
    for t in update.message.text.strip().split('\n'):
        try:
            db_execute("INSERT OR REPLACE INTO schedules VALUES (?, ?)", (t.strip(), pid))
            h, m = map(int, t.strip().split(':'))
            scheduler.add_job(run_async_job, 'cron', hour=h, minute=m, args=[context.bot, pid], id=f"{t}_{pid}")
        except: continue
    await update.message.reply_text(f"✅ Đã đặt lịch bài #{pid}!", reply_markup=get_main_menu())
    return ConversationHandler.END

async def save_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        aid = int(update.message.text)
        db_execute("INSERT OR IGNORE INTO admins VALUES (?, ?)", (aid, f"Admin {aid}"))
        await update.message.reply_text(f"✅ Đã thêm Admin {aid}!", reply_markup=get_main_menu())
    except: await update.message.reply_text("❌ Lỗi ID.")
    return ConversationHandler.END

async def handle_my_chat_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chm = update.my_chat_member
    if not chm:
        return
    chat = chm.chat
    if chat.type not in ("group", "supergroup"):
        return
    status = chm.new_chat_member.status
    if status in ("member", "administrator"):
        db_execute("INSERT OR IGNORE INTO groups (id, title, active) VALUES (?, ?, 1)", (chat.id, chat.title or ""))
        db_execute("UPDATE groups SET title = ?, active = 1 WHERE id = ?", (chat.title or "", chat.id))
    elif status in ("left", "kicked"):
        db_execute("UPDATE groups SET active = 0 WHERE id = ?", (chat.id,))

async def track_groups(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update and update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
            db_execute("INSERT OR IGNORE INTO groups (id, title, active) VALUES (?, ?, 1)", (update.effective_chat.id, update.effective_chat.title))
    except:
        pass

async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    try:
        logging.error("Handler error", exc_info=context.error)
    except:
        pass

def reload_schedules(bot):
    for t, pid in db_execute("SELECT time, post_id FROM schedules", fetch=True):
        try:
            h, m = map(int, t.split(':'))
            scheduler.add_job(run_async_job, 'cron', hour=h, minute=m, args=[bot, pid], id=f"{t}_{pid}")
        except: pass

def run_bot(token):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    app = ApplicationBuilder().token(token).build()
    reload_schedules(app.bot)
    _bot_loops[id(app.bot)] = loop

    conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(handle_callback)],
        states={
            WAIT_POST: [MessageHandler(filters.ALL & ~filters.COMMAND, save_post_input)],
            WAIT_EDIT_POST: [MessageHandler(filters.ALL & ~filters.COMMAND, save_post_input)],
            WAIT_SCHED: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_sched_input)],
            WAIT_ADD_ADMIN: [MessageHandler(filters.TEXT & ~filters.COMMAND, save_admin_input)],
        },
        fallbacks=[CommandHandler('start', start), CallbackQueryHandler(handle_callback, pattern='to_main')],
        per_chat=True,
        per_user=True,
        per_message=False
    )
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(conv)
    app.add_handler(MessageHandler(filters.ChatType.GROUPS, track_groups))
    app.add_handler(ChatMemberHandler(handle_my_chat_member, chat_member_types=ChatMemberHandler.MY_CHAT_MEMBER))
    app.add_error_handler(on_error)
    
    bot_info = loop.run_until_complete(app.bot.get_me())
    print(f"Bot '{bot_info.username}' is running...")
    app.run_polling(stop_signals=None, close_loop=False)

if __name__ == '__main__':
    _start_http_server()
    if not scheduler.running: scheduler.start()
    try:
        _ensure_worker()
        wd = threading.Thread(target=_watchdog, daemon=True)
        wd.start()
    except:
        pass

    threads = []
    for token in TOKENS:
        thread = threading.Thread(target=run_bot, args=(token,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("Tất cả các bot đã dừng.")
