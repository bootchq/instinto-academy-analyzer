#!/bin/bash
# Универсальный entrypoint для Railway
# Читает переменную RUN_SCRIPT и запускает соответствующий скрипт

SCRIPT="${RUN_SCRIPT:-academy_bot.py}"
echo "Запускаю: python $SCRIPT"
exec python "$SCRIPT"
