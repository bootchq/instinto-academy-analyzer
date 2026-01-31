"""Общие функции для форматирования отчётов."""
from typing import Dict, List, Tuple


# Названия навыков для отчёта
SKILL_NAMES = {
    "greeting_score": "Приветствие",
    "needs_score": "Выявление потребностей",
    "presentation_score": "Презентация",
    "objection_score": "Работа с возражениями",
    "closing_score": "Закрытие сделки",
    "cross_sell_score": "Допродажа",
}


def calculate_skill_averages(skills: Dict[str, List[float]]) -> Dict[str, float]:
    """Считает средние по навыкам."""
    result = {}
    for skill_key, scores in skills.items():
        if scores:
            result[skill_key] = round(sum(scores) / len(scores), 1)
        else:
            result[skill_key] = 0.0
    return result


def find_weakest_skills(averages: Dict[str, float], top_n: int = 3) -> List[Tuple[str, float]]:
    """
    Находит N самых слабых навыков.
    Возвращает список (skill_key, average).
    """
    # Фильтруем нулевые (нет данных)
    non_zero = [(k, v) for k, v in averages.items() if v > 0]
    # Сортируем по возрастанию (слабые сначала)
    sorted_skills = sorted(non_zero, key=lambda x: x[1])
    return sorted_skills[:top_n]


def format_report(
    manager_name: str,
    chat_count: int,
    weakest: List[Tuple[str, float]],
    missed_examples: List[str],
) -> str:
    """Форматирует отчёт для Telegram."""
    lines = [
        f"<b>Твой отчёт за неделю</b>",
        "",
        f"Проанализировано чатов: {chat_count}",
        "",
        "<b>Точки роста:</b>",
    ]

    for i, (skill_key, avg) in enumerate(weakest, 1):
        skill_name = SKILL_NAMES.get(skill_key, skill_key)
        example = missed_examples[i - 1] if i - 1 < len(missed_examples) else ""

        lines.append(f"{i}. {skill_name} ({avg})")
        if example:
            # Обрезаем длинные примеры
            example_short = example[:100] + "..." if len(example) > 100 else example
            lines.append(f"   <i>» {example_short}</i>")

    return "\n".join(lines)
