#!/bin/sh
set -e

# Verifica se deve rodar as migrações
# if [ "$BACK_NAME" = "backend-1" ]; then
#   echo "Aplicando migrações no banco..."
#   alembic upgrade head
# else
#   echo "Ignorando migrações para $BACK_NAME"
# fi

# Inicia o FastAPI
exec uvicorn main:app --reload --host 0.0.0.0 --port 8000
