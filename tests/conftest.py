"""Общие фикстуры для тестов INSTINTO Academy."""

import os
import sys
import pytest
from unittest.mock import MagicMock
from datetime import date, datetime, timezone

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture
def sample_raw_chat_graphql():
    """Сырой чат из WebGraphQL с вложенной структурой lastDialog."""
    return {
        "id": "chat_123",
        "channel": {"type": "WHATSAPP", "name": "WhatsApp"},
        "customer": {"id": "client_789"},
        "lastDialog": {
            "createdAt": "2026-02-10T08:00:00Z",
            "responsible": {"id": "2494", "name": "Алина Петрова"},
            "closedAt": None,
        },
        "lastActivity": "2026-02-15T12:00:00Z",
    }


@pytest.fixture
def sample_messages():
    """Тестовые сообщения чата."""
    return [
        {"chat_id": "123", "message_id": "msg_1", "sent_at": "2026-02-10T10:00:00Z",
         "direction": "in", "manager_id": "", "text": "Добрый день! Интересует белье для невесты"},
        {"chat_id": "123", "message_id": "msg_2", "sent_at": "2026-02-10T10:01:00Z",
         "direction": "out", "manager_id": "2494", "text": "Здравствуйте! Расскажите о ваших предпочтениях"},
        {"chat_id": "123", "message_id": "msg_3", "sent_at": "2026-02-10T10:02:00Z",
         "direction": "in", "manager_id": "", "text": "Хочу что-то элегантное"},
        {"chat_id": "123", "message_id": "msg_4", "sent_at": "2026-02-10T10:03:00Z",
         "direction": "out", "manager_id": "2494", "text": "Рекомендую коллекцию Bride"},
    ]


@pytest.fixture
def manager_map():
    """Маппинг manager_id -> имя."""
    return {
        "2494": "Алина Петрова",
        "1234": "Мария Сидорова",
        "5678": "Елена Козлова",
    }


@pytest.fixture
def chats_header():
    """Заголовок листа chats_raw."""
    return [
        "chat_id", "channel", "manager_id", "manager_name", "client_id", "order_id",
        "has_order", "payment_status", "payment_status_ru", "is_successful",
        "order_count", "status", "created_at", "outcome",
        "inbound_count", "outbound_count", "first_response_sec", "unanswered_inbound",
        "is_closed",
    ]


@pytest.fixture
def analysis_header():
    """Заголовок листа analysis_raw."""
    return [
        "chat_id", "manager_id", "manager_name", "channel",
        "message_count", "chat_status",
        "customer_segment", "overall_score",
        "greeting_score", "needs_score", "presentation_score",
        "objection_score", "closing_score", "cross_sell_score",
        "techniques", "missed_opportunities", "is_ethical", "summary",
        "analyzed_at",
    ]
