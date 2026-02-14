"""
Flask API для веб-авторизации академии INSTINTO.

Endpoints:
- POST /api/request-access - подать заявку на доступ
- POST /api/login - войти с логином/паролем
- GET /api/check-auth - проверить токен

Запускается вместе с Telegram ботом в отдельном потоке.
"""

import os
import secrets
import hashlib
import logging
from datetime import datetime, timedelta
from functools import wraps

import bcrypt
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify
from flask_cors import CORS
import jwt
import requests

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Flask приложение
app = Flask(__name__)
CORS(app, origins=["https://academy-modules.vercel.app", "http://localhost:*"])

# Секретный ключ для JWT
JWT_SECRET = os.environ.get("JWT_SECRET")
if not JWT_SECRET:
    logger.error("КРИТИЧНО: JWT_SECRET не установлен в переменных окружения!")
    raise ValueError("JWT_SECRET обязателен для работы веб-авторизации")
JWT_EXPIRY_HOURS = 24 * 7  # Токен на неделю

# Telegram для уведомлений
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
ADMIN_CHAT_ID = 57186925

# Database
DATABASE_URL = os.environ.get("DATABASE_URL", "")


def get_db():
    """Получает соединение с базой данных."""
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def run_migrations():
    """Выполняет миграции базы данных."""
    try:
        conn = get_db()
        cur = conn.cursor()
        # Добавляем колонку phone если её нет
        cur.execute("ALTER TABLE web_access_requests ADD COLUMN IF NOT EXISTS phone VARCHAR(50)")
        # Удаляем тестового пользователя если есть
        cur.execute("DELETE FROM web_users WHERE login = 'test_user_123'")
        # Таблица прогресса студентов
        cur.execute("""
            CREATE TABLE IF NOT EXISTS web_progress (
                id SERIAL PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES web_users(id) ON DELETE CASCADE,
                module_id INTEGER NOT NULL,
                score INTEGER,
                passed BOOLEAN DEFAULT FALSE,
                time_spent_seconds INTEGER DEFAULT 0,
                completed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (user_id, module_id)
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Миграции выполнены")
    except Exception as e:
        logger.warning(f"Миграции пропущены: {e}")


def hash_password_bcrypt(password: str) -> str:
    """Хеширует пароль используя bcrypt."""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')


def hash_password_sha256_legacy(password: str) -> str:
    """Старый SHA256 хеш (только для проверки совместимости)."""
    return hashlib.sha256(password.encode()).hexdigest()


def verify_password(password: str, stored_hash: str) -> bool:
    """
    Проверяет пароль против хеша (поддерживает SHA256 и bcrypt).

    Автоматически определяет формат по длине/префиксу:
    - bcrypt: начинается с $2b$ (60 символов)
    - SHA256: hex строка (64 символа)
    """
    if not stored_hash:
        return False

    # Проверяем формат bcrypt
    if stored_hash.startswith('$2b$') or stored_hash.startswith('$2a$'):
        try:
            return bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8'))
        except Exception as e:
            logger.error(f"Ошибка проверки bcrypt пароля: {e}")
            return False

    # Fallback на старый SHA256 (для совместимости)
    elif len(stored_hash) == 64 and all(c in '0123456789abcdef' for c in stored_hash):
        legacy_hash = hash_password_sha256_legacy(password)
        return legacy_hash == stored_hash

    # Неизвестный формат
    logger.warning(f"Неизвестный формат хеша пароля (длина={len(stored_hash)})")
    return False


def generate_credentials():
    """Генерирует логин и пароль."""
    login = f"student_{secrets.token_hex(4)}"
    password = secrets.token_urlsafe(8)
    return login, password


def create_jwt_token(user_id: int, login: str, role: str) -> str:
    """Создаёт JWT токен."""
    payload = {
        "user_id": user_id,
        "login": login,
        "role": role,
        "exp": datetime.utcnow() + timedelta(hours=JWT_EXPIRY_HOURS)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def verify_jwt_token(token: str) -> dict:
    """Проверяет JWT токен."""
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def require_auth(f):
    """Декоратор для проверки авторизации."""
    @wraps(f)
    def decorated(*args, **kwargs):
        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            return jsonify({"error": "Требуется авторизация"}), 401

        token = auth_header.split(" ")[1]
        payload = verify_jwt_token(token)
        if not payload:
            return jsonify({"error": "Недействительный токен"}), 401

        request.user = payload
        return f(*args, **kwargs)
    return decorated


def send_telegram_notification(chat_id: int, text: str, reply_markup: dict = None):
    """Отправляет уведомление в Telegram."""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN не задан")
        return False

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup

    try:
        response = requests.post(url, json=payload, timeout=10)
        return response.ok
    except Exception as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")
        return False


# === API Endpoints ===

@app.route("/api/health", methods=["GET"])
def health():
    """Проверка работоспособности."""
    return jsonify({"status": "ok", "service": "academy-auth"})


@app.route("/api/request-access", methods=["POST"])
def request_access():
    """Подать заявку на доступ."""
    data = request.get_json()
    telegram_username = data.get("telegram", "").strip().replace("@", "")

    if not telegram_username:
        return jsonify({"error": "Укажите Telegram username"}), 400

    try:
        conn = get_db()
        cur = conn.cursor()

        # Проверяем, нет ли уже такой заявки
        cur.execute(
            "SELECT id, status FROM web_access_requests WHERE telegram_username = %s ORDER BY created_at DESC LIMIT 1",
            (telegram_username,)
        )
        existing = cur.fetchone()

        if existing:
            if existing["status"] == "pending":
                return jsonify({"message": "Заявка уже отправлена, ожидайте одобрения"}), 200
            elif existing["status"] == "approved":
                return jsonify({"message": "Вы уже одобрены, проверьте Telegram для получения данных"}), 200

        # Проверяем, есть ли уже пользователь
        cur.execute(
            "SELECT id FROM web_users WHERE telegram_username = %s",
            (telegram_username,)
        )
        if cur.fetchone():
            return jsonify({"message": "У вас уже есть доступ, используйте логин/пароль из Telegram"}), 200

        # Создаём заявку
        cur.execute(
            "INSERT INTO web_access_requests (telegram_username, status) VALUES (%s, 'pending') RETURNING id",
            (telegram_username,)
        )
        request_id = cur.fetchone()["id"]
        conn.commit()

        # Уведомляем админа
        text = (
            f"<b>Новая заявка на доступ к Академии</b>\n\n"
            f"Telegram: @{telegram_username}\n"
            f"ID заявки: {request_id}"
        )
        reply_markup = {
            "inline_keyboard": [[
                {"text": "Одобрить", "callback_data": f"web_approve:{request_id}"},
                {"text": "Отклонить", "callback_data": f"web_reject:{request_id}"}
            ]]
        }
        send_telegram_notification(ADMIN_CHAT_ID, text, reply_markup)

        cur.close()
        conn.close()

        return jsonify({
            "success": True,
            "message": "Заявка отправлена! Ожидайте уведомления в Telegram."
        }), 200

    except Exception as e:
        logger.error(f"Ошибка создания заявки: {e}")
        return jsonify({"error": "Ошибка сервера"}), 500


@app.route("/api/login", methods=["POST"])
def login():
    """Войти с логином/паролем."""
    data = request.get_json()
    login_value = data.get("login", "").strip()
    password = data.get("password", "").strip()

    if not login_value or not password:
        return jsonify({"error": "Укажите логин и пароль"}), 400

    try:
        conn = get_db()
        cur = conn.cursor()

        # Получаем пользователя по логину (без проверки пароля)
        cur.execute(
            "SELECT id, login, role, password_hash FROM web_users WHERE login = %s",
            (login_value,)
        )
        user = cur.fetchone()

        if not user:
            return jsonify({"error": "Неверный логин или пароль"}), 401

        # Проверяем пароль (поддерживаем SHA256 и bcrypt)
        if not verify_password(password, user["password_hash"]):
            return jsonify({"error": "Неверный логин или пароль"}), 401

        # Если пользователь использовал старый SHA256 хеш — обновляем на bcrypt
        stored_hash = user["password_hash"]
        is_legacy_hash = len(stored_hash) == 64 and all(c in '0123456789abcdef' for c in stored_hash)

        if is_legacy_hash:
            logger.info(f"Обновляю устаревший SHA256 хеш на bcrypt для пользователя {login_value}")
            new_hash = hash_password_bcrypt(password)
            cur.execute(
                "UPDATE web_users SET password_hash = %s WHERE id = %s",
                (new_hash, user["id"])
            )

        # Обновляем last_login
        cur.execute(
            "UPDATE web_users SET last_login = CURRENT_TIMESTAMP WHERE id = %s",
            (user["id"],)
        )
        conn.commit()

        # Создаём токен
        token = create_jwt_token(user["id"], user["login"], user["role"])

        cur.close()
        conn.close()

        return jsonify({
            "success": True,
            "token": token,
            "user": {
                "login": user["login"],
                "role": user["role"]
            }
        }), 200

    except Exception as e:
        logger.error(f"Ошибка авторизации: {e}")
        return jsonify({"error": "Ошибка сервера"}), 500


@app.route("/api/check-auth", methods=["GET"])
@require_auth
def check_auth():
    """Проверить авторизацию."""
    return jsonify({
        "authenticated": True,
        "user": {
            "login": request.user["login"],
            "role": request.user["role"]
        }
    }), 200


@app.route("/api/progress", methods=["POST"])
@require_auth
def save_progress():
    """Сохраняет прогресс студента по модулю."""
    data = request.get_json()
    module_id = data.get("module_id")
    score = data.get("score")
    passed = data.get("passed", False)
    time_spent = data.get("time_spent_seconds", 0)

    if not isinstance(module_id, int) or module_id < 1 or module_id > 14:
        return jsonify({"error": "Некорректный module_id"}), 400

    user_id = request.user["user_id"]

    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO web_progress (user_id, module_id, score, passed, time_spent_seconds, completed_at)
            VALUES (%s, %s, %s, %s, %s, CASE WHEN %s THEN CURRENT_TIMESTAMP ELSE NULL END)
            ON CONFLICT (user_id, module_id) DO UPDATE SET
                score = GREATEST(web_progress.score, EXCLUDED.score),
                passed = EXCLUDED.passed OR web_progress.passed,
                time_spent_seconds = web_progress.time_spent_seconds + EXCLUDED.time_spent_seconds,
                completed_at = CASE
                    WHEN EXCLUDED.passed AND web_progress.completed_at IS NULL THEN CURRENT_TIMESTAMP
                    ELSE web_progress.completed_at
                END,
                updated_at = CURRENT_TIMESTAMP
        """, (user_id, module_id, score, passed, time_spent, passed))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"success": True}), 200
    except Exception as e:
        logger.error(f"Ошибка сохранения прогресса: {e}")
        return jsonify({"error": "Ошибка сервера"}), 500


@app.route("/api/progress", methods=["GET"])
@require_auth
def get_progress():
    """Возвращает прогресс текущего студента."""
    user_id = request.user["user_id"]

    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "SELECT module_id, score, passed, time_spent_seconds, completed_at FROM web_progress WHERE user_id = %s",
            (user_id,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()

        progress = {}
        for row in rows:
            progress[row["module_id"]] = {
                "score": row["score"],
                "passed": row["passed"],
                "time_spent_seconds": row["time_spent_seconds"],
                "date": row["completed_at"].isoformat() if row["completed_at"] else None
            }

        return jsonify({"progress": progress}), 200
    except Exception as e:
        logger.error(f"Ошибка получения прогресса: {e}")
        return jsonify({"error": "Ошибка сервера"}), 500


@app.route("/api/admin/progress", methods=["GET"])
@require_auth
def get_admin_progress():
    """Возвращает прогресс всех студентов (только для admin)."""
    if request.user.get("role") != "admin":
        return jsonify({"error": "Доступ запрещён"}), 403

    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                wu.id,
                wu.login,
                wu.telegram_username,
                war.phone,
                tu.full_name,
                COUNT(CASE WHEN wp.passed THEN 1 END) as modules_completed,
                ROUND(AVG(CASE WHEN wp.passed THEN wp.score END)::numeric, 1) as avg_score,
                COALESCE(SUM(wp.time_spent_seconds), 0) as total_time_seconds
            FROM web_users wu
            LEFT JOIN web_progress wp ON wu.id = wp.user_id
            LEFT JOIN web_access_requests war
                ON wu.telegram_username = war.telegram_username AND war.status = 'approved'
            LEFT JOIN telegram_users tu ON wu.telegram_username = tu.username
            WHERE wu.role = 'student'
            GROUP BY wu.id, wu.login, wu.telegram_username, war.phone, tu.full_name
            ORDER BY modules_completed DESC, avg_score DESC
        """)

        students = cur.fetchall()
        cur.close()
        conn.close()

        return jsonify({"students": [dict(s) for s in students]}), 200
    except Exception as e:
        logger.error(f"Ошибка получения прогресса студентов: {e}")
        return jsonify({"error": "Ошибка сервера"}), 500


# === Функции для бота ===

def approve_web_request(request_id: int) -> tuple:
    """
    Одобряет заявку и создаёт пользователя.
    Возвращает (telegram_username, telegram_id, login, password) или (None, None, None, None).
    """
    try:
        conn = get_db()
        cur = conn.cursor()

        # Получаем заявку
        cur.execute(
            "SELECT telegram_username FROM web_access_requests WHERE id = %s AND status = 'pending'",
            (request_id,)
        )
        req = cur.fetchone()

        if not req:
            return None, None, None, None

        telegram_username = req["telegram_username"]

        # Генерируем логин/пароль
        login, password = generate_credentials()
        password_hash = hash_password_bcrypt(password)

        # Создаём пользователя
        cur.execute(
            "INSERT INTO web_users (telegram_username, login, password_hash, role) VALUES (%s, %s, %s, 'student')",
            (telegram_username, login, password_hash)
        )

        # Обновляем статус заявки
        cur.execute(
            "UPDATE web_access_requests SET status = 'approved', processed_at = CURRENT_TIMESTAMP WHERE id = %s",
            (request_id,)
        )

        # Ищем telegram_id пользователя
        username_clean = telegram_username.replace("@", "").replace("+", "")
        cur.execute(
            "SELECT telegram_id FROM telegram_users WHERE username = %s OR username = %s",
            (username_clean, telegram_username)
        )
        tg_user = cur.fetchone()
        telegram_id = tg_user["telegram_id"] if tg_user else None

        conn.commit()
        cur.close()
        conn.close()

        return telegram_username, telegram_id, login, password

    except Exception as e:
        logger.error(f"Ошибка одобрения заявки: {e}")
        return None, None, None, None


def save_telegram_user(telegram_id: int, username: str, full_name: str):
    """Сохраняет telegram_id пользователя для последующей отправки сообщений."""
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO telegram_users (telegram_id, username, full_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (telegram_id) DO UPDATE SET
                username = EXCLUDED.username,
                full_name = EXCLUDED.full_name
        """, (telegram_id, username, full_name))

        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения telegram user: {e}")
        return False


def get_telegram_id_by_username(username: str) -> int:
    """Получает telegram_id по username."""
    try:
        conn = get_db()
        cur = conn.cursor()

        # Убираем @ если есть
        username = username.replace("@", "").replace("+", "")

        cur.execute(
            "SELECT telegram_id FROM telegram_users WHERE username = %s OR username = %s",
            (username, f"+{username}")
        )
        result = cur.fetchone()

        cur.close()
        conn.close()

        return result["telegram_id"] if result else None
    except Exception as e:
        logger.error(f"Ошибка получения telegram_id: {e}")
        return None


def reject_web_request(request_id: int) -> str:
    """
    Отклоняет заявку.
    Возвращает telegram_username или None.
    """
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute(
            "SELECT telegram_username FROM web_access_requests WHERE id = %s AND status = 'pending'",
            (request_id,)
        )
        req = cur.fetchone()

        if not req:
            return None

        cur.execute(
            "UPDATE web_access_requests SET status = 'rejected', processed_at = CURRENT_TIMESTAMP WHERE id = %s",
            (request_id,)
        )

        conn.commit()
        cur.close()
        conn.close()

        return req["telegram_username"]

    except Exception as e:
        logger.error(f"Ошибка отклонения заявки: {e}")
        return None


def run_api_server(host="0.0.0.0", port=5000):
    """Запускает Flask сервер."""
    run_migrations()
    logger.info(f"Запуск API сервера на {host}:{port}")
    app.run(host=host, port=port, threaded=True)


if __name__ == "__main__":
    # Для локального тестирования
    run_api_server()
