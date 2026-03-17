# db.py - Updated (Fixes DB Locks & Race Conditions)
import sqlite3, time, asyncio, logging
from datetime import datetime
import uuid
import config

logger = logging.getLogger(__name__)
_lock = asyncio.Lock() 

def calculate_elo(player_rating, opponent_rating, score, k_factor=32):
    expected_score = 1 / (1 + 10**((opponent_rating - player_rating) / 400))
    new_rating = player_rating + k_factor * (score - expected_score)
    return int(round(new_rating))

def get_conn():
    # প্রতিবার নতুন কানেকশন তৈরি করা হচ্ছে ডেটাবেস লক (Database locked) সমস্যা সমাধানের জন্য
    conn = sqlite3.connect(config.LOCAL_DB, timeout=20, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def _add_column_if_not_exists(cursor, table_name, column_name, column_type_with_default):
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row['name'] for row in cursor.fetchall()]
    if column_name not in columns:
        logger.info(f"Adding column '{column_name}' to table '{table_name}'...")
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_with_default}")

def init_db():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, username TEXT, ingame_name TEXT, phone_number TEXT, is_registered INTEGER DEFAULT 0, balance REAL DEFAULT 0, welcome_given INTEGER DEFAULT 0, wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, created_at TIMESTAMP, state TEXT, state_data TEXT, referrer_id INTEGER, elo_rating INTEGER DEFAULT 1000)''')
        _add_column_if_not_exists(cur, 'users', 'elo_rating', 'INTEGER DEFAULT 1000')
        _add_column_if_not_exists(cur, 'users', 'is_banned', 'INTEGER DEFAULT 0')
        cur.execute('''CREATE TABLE IF NOT EXISTS deposit_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, txid TEXT, amount REAL, status TEXT DEFAULT 'pending', created_at INTEGER)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS withdrawal_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, method TEXT, account_number TEXT, status TEXT DEFAULT 'pending', created_at INTEGER)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS transactions (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, amount REAL, type TEXT, note TEXT, created_at INTEGER)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS matchmaking_queue (user_id INTEGER PRIMARY KEY, fee REAL, joined_at INTEGER, lobby_message_id INTEGER)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS active_matches (match_id TEXT PRIMARY KEY, player1_id INTEGER, player2_id INTEGER, fee REAL, status TEXT, room_code TEXT, created_at INTEGER, p1_screenshot_id TEXT, p2_screenshot_id TEXT, winner_id INTEGER)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)''')
        conn.commit()

async def run_db(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

# --- Settings ---
def get_setting_sync(key):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = cur.fetchone()
        return row['value'] if row else None
async def get_setting(key): return await run_db(get_setting_sync, key)

def set_setting_sync(key, value):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value))
        conn.commit()
async def set_setting(key, value): await run_db(set_setting_sync, key, value)

def get_all_user_ids_sync():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE is_registered = 1 AND is_banned = 0")
        return [row['user_id'] for row in cur.fetchall()]
async def get_all_user_ids(): return await run_db(get_all_user_ids_sync)

# --- Users ---
def create_user_if_not_exists_sync(user_id, username, referrer_id=None): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('INSERT OR IGNORE INTO users(user_id, username, created_at, elo_rating) VALUES(?,?,?,?)', (user_id, username, datetime.now(), 1000))
        if referrer_id:
            cur.execute("SELECT changes()")
            if cur.fetchone()[0] > 0:
                cur.execute("UPDATE users SET referrer_id = ? WHERE user_id = ?", (referrer_id, user_id))
        conn.commit()
async def create_user_if_not_exists(user_id, username, referrer_id=None): await run_db(create_user_if_not_exists_sync, user_id, username, referrer_id)

def get_user_sync(user_id): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM users WHERE user_id=?',(user_id,))
        r = cur.fetchone()
        return dict(r) if r else None
async def get_user(user_id): return await run_db(get_user_sync, user_id)

def update_user_fields_sync(user_id, data): 
    with get_conn() as conn:
        cur = conn.cursor()
        sets = ','.join([f"{k}=?" for k in data.keys()])
        params = list(data.values()) + [user_id]
        cur.execute(f'UPDATE users SET {sets} WHERE user_id=?', params)
        conn.commit()
async def update_user_fields(user_id, data): await run_db(update_user_fields_sync, user_id, data)

async def set_user_state(user_id, state, state_data=None): await update_user_fields(user_id, {'state': state, 'state_data': state_data})

def adjust_balance_sync(user_id, amount, tx_type='adjust', note=''): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE users SET balance=balance+? WHERE user_id=?',(amount,user_id))
        cur.execute('INSERT INTO transactions(user_id, amount, type, note, created_at) VALUES(?,?,?,?,?)',(user_id, amount, tx_type, note, int(time.time())))
        conn.commit()
async def adjust_balance(user_id, amount, tx_type='adjust', note=''): await run_db(adjust_balance_sync, user_id, amount, tx_type, note)

# --- Matchmaking Queue ---
def add_to_queue_sync(user_id, fee, lobby_message_id): 
    with get_conn() as conn:
        cur = conn.cursor()
        # ব্যালেন্স হ্যাক বন্ধ করতে Queue-তে ঢোকার সময়ই ফি কাটা হচ্ছে
        if fee > 0:
            cur.execute('UPDATE users SET balance=balance-? WHERE user_id=?',(fee, user_id))
            cur.execute('INSERT INTO transactions(user_id, amount, type, note, created_at) VALUES(?,?,?,?,?)',(user_id, -fee, 'queue_fee', 'Joined matchmaking', int(time.time())))
        cur.execute('INSERT OR REPLACE INTO matchmaking_queue(user_id,fee,joined_at,lobby_message_id) VALUES(?,?,?,?)',(user_id,fee,int(time.time()),lobby_message_id))
        conn.commit()
async def add_to_queue(user_id, fee, lobby_message_id): await run_db(add_to_queue_sync, user_id, fee, lobby_message_id)

def get_from_queue_sync(user_id): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM matchmaking_queue WHERE user_id=?',(user_id,))
        r = cur.fetchone()
        return dict(r) if r else None
async def get_from_queue(user_id): return await run_db(get_from_queue_sync, user_id)

def remove_from_queue_sync(user_id, refund=False): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT fee FROM matchmaking_queue WHERE user_id=?', (user_id,))
        row = cur.fetchone()
        if row:
            fee = row['fee']
            # যদি ক্যানসেল করা হয়, তাহলে টাকা রিফান্ড হবে
            if refund and fee > 0:
                cur.execute('UPDATE users SET balance=balance+? WHERE user_id=?',(fee, user_id))
                cur.execute('INSERT INTO transactions(user_id, amount, type, note, created_at) VALUES(?,?,?,?,?)',(user_id, fee, 'queue_refund', 'Left matchmaking', int(time.time())))
            cur.execute('DELETE FROM matchmaking_queue WHERE user_id=?',(user_id,))
            conn.commit()
            return True
        return False
async def remove_from_queue(user_id, refund=False): return await run_db(remove_from_queue_sync, user_id, refund)

def find_opponent_in_queue_sync(fee, player_id_to_exclude):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM matchmaking_queue WHERE fee = ? AND user_id != ? ORDER BY joined_at ASC LIMIT 1', (fee, player_id_to_exclude))
        r = cur.fetchone()
        return dict(r) if r else None
async def find_opponent_in_queue(fee, player_id_to_exclude): return await run_db(find_opponent_in_queue_sync, fee, player_id_to_exclude)

# --- Active Matches ---
def create_match_sync(p1_id, p2_id, fee, deduct_p1=True): 
    with get_conn() as conn:
        cur = conn.cursor()
        match_id = str(uuid.uuid4())[:8]
        cur.execute('INSERT INTO active_matches(match_id, player1_id, player2_id, fee, status, created_at) VALUES(?,?,?,?,?,?)',(match_id, p1_id, p2_id, fee, 'waiting_for_code', int(time.time())))
        # Player 1 মাত্র বাটন ক্লিক করেছে, তাই তার ফি এখন কাটা হবে (P2 এর ফি আগেই queue-তে কাটা হয়েছে)
        if deduct_p1 and fee > 0:
            cur.execute('UPDATE users SET balance=balance-? WHERE user_id=?',(fee, p1_id))
            cur.execute('INSERT INTO transactions(user_id, amount, type, note, created_at) VALUES(?,?,?,?,?)',(p1_id, -fee, 'match_entry', f'Match {match_id}', int(time.time())))
        conn.commit()
        return match_id
async def create_match(p1_id, p2_id, fee, deduct_p1=True): return await run_db(create_match_sync, p1_id, p2_id, fee, deduct_p1)

def set_room_code_sync(match_id, room_code): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE active_matches SET room_code=?, status='in_progress' WHERE match_id=?", (room_code, match_id))
        conn.commit()
async def set_room_code(match_id, room_code): await run_db(set_room_code_sync, match_id, room_code)

def get_match_sync(match_id): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM active_matches WHERE match_id=?',(match_id,))
        r = cur.fetchone()
        return dict(r) if r else None
async def get_match(match_id): return await run_db(get_match_sync, match_id)

def submit_screenshot_sync(match_id, player_id, screenshot_id): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT player1_id FROM active_matches WHERE match_id=?', (match_id,))
        match = cur.fetchone()
        if not match: return None
        field_to_update = 'p1_screenshot_id' if player_id == match['player1_id'] else 'p2_screenshot_id'
        cur.execute(f'UPDATE active_matches SET {field_to_update}=? WHERE match_id=?',(screenshot_id, match_id))
        conn.commit()
        cur.execute('SELECT * FROM active_matches WHERE match_id=?',(match_id,))
        return dict(cur.fetchone())
async def submit_screenshot(match_id, player_id, screenshot_id): return await run_db(submit_screenshot_sync, match_id, player_id, screenshot_id)

def resolve_match_sync(match_id, winner_id): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM active_matches WHERE match_id=?', (match_id,))
        match_row = cur.fetchone()
        if not match_row or dict(match_row)['status'] == 'completed': return False
        
        match = dict(match_row)
        p1_id, p2_id, fee = match['player1_id'], match['player2_id'], match['fee']
        loser_id = p2_id if winner_id == p1_id else p1_id
        
        cur.execute('SELECT elo_rating FROM users WHERE user_id=?', (winner_id,))
        w_res = cur.fetchone()
        cur.execute('SELECT elo_rating FROM users WHERE user_id=?', (loser_id,))
        l_res = cur.fetchone()
        
        if w_res and l_res:
            winner_new_elo = calculate_elo(w_res['elo_rating'], l_res['elo_rating'], 1)
            loser_new_elo = calculate_elo(l_res['elo_rating'], w_res['elo_rating'], 0)
            cur.execute('UPDATE users SET elo_rating = ? WHERE user_id = ?', (winner_new_elo, winner_id))
            cur.execute('UPDATE users SET elo_rating = ? WHERE user_id = ?', (loser_new_elo, loser_id))
        
        if fee > 0:
            prize = fee * 2 * 0.9
            cur.execute('UPDATE users SET balance=balance+? WHERE user_id=?', (prize, winner_id))
            cur.execute('INSERT INTO transactions(user_id, amount, type, note, created_at) VALUES(?,?,?,?,?)',(winner_id, prize, 'match_win', f'Won match {match_id}', int(time.time())))
            
        cur.execute('UPDATE users SET wins = wins + 1 WHERE user_id=?',(winner_id,))
        cur.execute('UPDATE users SET losses = losses + 1 WHERE user_id=?',(loser_id,))
        cur.execute("UPDATE active_matches SET status='completed', winner_id=? WHERE match_id=?",(winner_id, match_id))
        conn.commit()
        return True
async def resolve_match(match_id, winner_id): return await run_db(resolve_match_sync, match_id, winner_id)

def cancel_match_sync(match_id): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE active_matches SET status='cancelled' WHERE match_id=?", (match_id,))
        conn.commit()
async def cancel_match(match_id): await run_db(cancel_match_sync, match_id)

# --- Leaderboard & Stats ---
def get_top_wins_sync(limit=10): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT ingame_name, username, wins, elo_rating FROM users WHERE is_registered=1 AND is_banned=0 ORDER BY elo_rating DESC, wins DESC LIMIT ?',(limit,))
        return [dict(r) for r in cur.fetchall()]
async def get_top_wins(limit=10): return await run_db(get_top_wins_sync, limit)

def get_total_users_sync():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as count FROM users WHERE is_registered = 1")
        return cur.fetchone()['count']
async def get_total_users(): return await run_db(get_total_users_sync)

def get_active_users_sync():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(DISTINCT user_id) as count FROM transactions WHERE created_at > ?", (int(time.time()) - 7*86400,))
        return cur.fetchone()['count']
async def get_active_users(): return await run_db(get_active_users_sync)

def get_total_matches_sync():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as count FROM active_matches WHERE status = 'completed'")
        return cur.fetchone()['count']
async def get_total_matches(): return await run_db(get_total_matches_sync)

def get_pending_deposits_count_sync():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as count FROM deposit_requests WHERE status = 'pending'")
        return cur.fetchone()['count']
async def get_pending_deposits_count(): return await run_db(get_pending_deposits_count_sync)

def get_pending_withdrawals_count_sync():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) as count FROM withdrawal_requests WHERE status = 'pending'")
        return cur.fetchone()['count']
async def get_pending_withdrawals_count(): return await run_db(get_pending_withdrawals_count_sync)

def get_total_fees_collected_sync():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT SUM(fee) as total FROM active_matches WHERE fee > 0 AND status = 'completed'")
        result = cur.fetchone()['total']
        return result or 0
async def get_total_fees_collected(): return await run_db(get_total_fees_collected_sync)

# --- Deposits & Withdrawals ---
def create_deposit_request_sync(user_id, txid, amount): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('INSERT INTO deposit_requests(user_id,txid,amount,created_at) VALUES(?,?,?,?)',(user_id,txid,amount,int(time.time())))
        conn.commit()
        return cur.lastrowid
async def create_deposit_request(user_id, txid, amount): return await run_db(create_deposit_request_sync, user_id, txid, amount)

def get_deposit_request_sync(req_id): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM deposit_requests WHERE id=?', (req_id,))
        r = cur.fetchone()
        return dict(r) if r else None
async def get_deposit_request(req_id): return await run_db(get_deposit_request_sync, req_id)

def update_deposit_status_sync(req_id, status): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE deposit_requests SET status=? WHERE id=?',(status, req_id))
        conn.commit()
async def update_deposit_status(req_id, status): await run_db(update_deposit_status_sync, req_id, status)

def create_withdrawal_request_sync(user_id, amount, method, account_number): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('INSERT INTO withdrawal_requests(user_id, amount, method, account_number, created_at) VALUES(?,?,?,?,?)', (user_id, amount, method, account_number, int(time.time())))
        conn.commit()
        return cur.lastrowid
async def create_withdrawal_request(user_id, amount, method, account_number): return await run_db(create_withdrawal_request_sync, user_id, amount, method, account_number)

def get_withdrawal_request_sync(req_id): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM withdrawal_requests WHERE id=?', (req_id,))
        r = cur.fetchone()
        return dict(r) if r else None
async def get_withdrawal_request(req_id): return await run_db(get_withdrawal_request_sync, req_id)

def update_withdrawal_status_sync(req_id, status): 
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute('UPDATE withdrawal_requests SET status=? WHERE id=?',(status, req_id))
        conn.commit()
async def update_withdrawal_status(req_id, status): await run_db(update_withdrawal_status_sync, req_id, status)