#!/bin/bash
set -e

echo "🔄 Waiting for PostgreSQL to be ready..."

MAX_RETRIES=30
COUNTER=0

until PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U "$DB_USER" -p "$DB_PORT" -c '\q' >/dev/null 2>&1; do
  COUNTER=$((COUNTER+1))
  if [ $COUNTER -ge $MAX_RETRIES ]; then
    echo "❌ PostgreSQL is not available after $MAX_RETRIES attempts. Exiting..."
    exit 1
  fi
  echo "⏳ Postgres is unavailable - sleeping ($COUNTER/$MAX_RETRIES)"
  sleep 2
done

echo "✅ PostgreSQL is up and running"

# Check if the role exists
USER_EXISTS=$(PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U postgres -p "$DB_PORT" -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'")
if [ "$USER_EXISTS" != "1" ]; then
  echo "👤 User '$DB_USER' does not exist. Creating..."
  PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U postgres -p "$DB_PORT" -c "CREATE ROLE ${DB_USER} WITH LOGIN PASSWORD '${DB_PASSWORD}';"
  PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U postgres -p "$DB_PORT" -c "ALTER ROLE ${DB_USER} CREATEDB;"
else
  echo "✅ User '$DB_USER' already exists"
fi

# Check if database exists
DB_EXISTS=$(PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U postgres -p "$DB_PORT" -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'")
if [ "$DB_EXISTS" != "1" ]; then
  echo "📦 Database '$DB_NAME' does not exist. Creating..."
  PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U postgres -p "$DB_PORT" -c "CREATE DATABASE ${DB_NAME} OWNER ${DB_USER};"
else
  echo "✅ Database '$DB_NAME' already exists"
fi

# Grant privileges
PGPASSWORD=$DB_PASSWORD psql -h "$DB_HOST" -U postgres -p "$DB_PORT" -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};"

# Run migrations automatically
echo "🚀 Running Django migrations..."
python manage.py makemigrations --noinput
python manage.py migrate --noinput

# Start Gunicorn in the background
echo "🔥 Starting Gunicorn server..."
gunicorn --bind 0.0.0.0:8000 --workers 3 --timeout 120 main.wsgi:application &
GUNICORN_PID=$!

# Optional: internal health check
echo "⏳ Waiting for Django to be ready..."
until curl -s http://localhost:8000/api/docs/ >/dev/null; do
  echo "🌐 Django not ready yet..."
  sleep 3
done
echo "✅ Django is responding."

# Bring Gunicorn back to foreground
wait $GUNICORN_PID
