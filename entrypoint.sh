#!/bin/bash

# Source environment variables
. /app/.env

# Strip quotes from variables if present
POSTGRES_USERNAME=$(echo $POSTGRES_USERNAME | sed 's/^"//' | sed 's/"$//')
POSTGRES_PASSWORD=$(echo $POSTGRES_PASSWORD | sed 's/^"//' | sed 's/"$//')

# Start PostgreSQL
su postgres -c "/usr/lib/postgresql/16/bin/pg_ctl -D /var/lib/postgresql/data -l /var/lib/postgresql/logfile start"

# Wait for PostgreSQL to start
sleep 5

# Create database if it doesn't exist
su postgres -c "/usr/lib/postgresql/16/bin/createdb bookrover" 2>/dev/null || true

# Create extension in the database
su postgres -c "/usr/lib/postgresql/16/bin/psql -c \"CREATE EXTENSION IF NOT EXISTS vector;\" bookrover"

# Create user and grant privileges if user doesn't exist
su postgres -c "/usr/lib/postgresql/16/bin/psql -c \"CREATE USER $POSTGRES_USERNAME WITH PASSWORD '$POSTGRES_PASSWORD';\" " 2>/dev/null || true
su postgres -c "/usr/lib/postgresql/16/bin/psql -c \"GRANT ALL PRIVILEGES ON DATABASE bookrover TO $POSTGRES_USERNAME;\" bookrover"
su postgres -c "/usr/lib/postgresql/16/bin/psql -c \"GRANT USAGE ON SCHEMA public TO $POSTGRES_USERNAME; GRANT CREATE ON SCHEMA public TO $POSTGRES_USERNAME;\" bookrover"

# Start supervisord
exec /usr/bin/supervisord -c /etc/supervisord.conf