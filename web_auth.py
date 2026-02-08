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
JWT_SECRET = os.environ.get("JWT_SECRET", "instinto-academy-secret-key-2024")
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
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Миграции выполнены")
    except Exception as e:
        logger.warning(f"Миграции пропущены: {e}")


def hash_password(password: str) -> str:
    """Хеширует пароль."""
    return hashlib.sha256(password.encode()).hexdigest()


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


@app.route("/api/clear-auth", methods=["POST"])
def clear_auth():
    """Очищает все данные авторизации (только для тестирования)."""
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("DELETE FROM web_users")
        users = cur.rowcount

        cur.execute("DELETE FROM web_access_requests")
        requests = cur.rowcount

        cur.execute("DELETE FROM telegram_users")
        tg = cur.rowcount

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({
            "success": True,
            "deleted": {"web_users": users, "web_access_requests": requests, "telegram_users": tg}
        })
    except Exception as e:
        logger.error(f"Ошибка очистки: {e}")
        return jsonify({"error": str(e)}), 500


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

        password_hash = hash_password(password)
        cur.execute(
            "SELECT id, login, role FROM web_users WHERE login = %s AND password_hash = %s",
            (login_value, password_hash)
        )
        user = cur.fetchone()

        if not user:
            return jsonify({"error": "Неверный логин или пароль"}), 401

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
        password_hash = hash_password(password)

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
