FROM ubuntu:24.04

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    nodejs \
    npm \
    postgresql-16 \
    postgresql-client-16 \
    postgresql-contrib \
    supervisor \
    postgresql-server-dev-16 \
    gcc \
    build-essential \
    git \
    make \
    && rm -rf /var/lib/apt/lists/*

# Install pgvector
RUN git clone https://github.com/pgvector/pgvector.git /tmp/pgvector \
    && cd /tmp/pgvector \
    && make \
    && make install \
    && rm -rf /tmp/pgvector

# Set up PostgreSQL
RUN mkdir -p /var/lib/postgresql/data /run/postgresql \
    && chown -R postgres:postgres /var/lib/postgresql /run/postgresql

# Init db as postgres user
RUN su postgres -c "/usr/lib/postgresql/16/bin/initdb -D /var/lib/postgresql/data"

# Set working directory
WORKDIR /app

# Copy Python project files
COPY pyproject.toml ./
COPY shared_python/ ./shared_python/
COPY engine/ ./engine/
COPY scraper/ ./scraper/

# Install Python dependencies
RUN pip install --no-cache-dir --break-system-packages \
    "anthropic>=0.84.0" \
    "python-dotenv>=1.0.0" \
    "einops>=0.8.2" \
    "flask>=3.1.3" \
    "flask-cors>=6.0.2" \
    "google>=3.0.0" \
    "google-genai>=1.66.0" \
    "nltk>=3.9.2" \
    "openai>=2.21.0" \
    "psycopg[c]>=3.1" \
    "pgvector>=0.4.2" \
    "sentence-transformers>=5.2.2"

# Copy Node.js project files
COPY package.json ./
COPY tsconfig.json ./
COPY webserver/ ./webserver/

# Install Node.js dependencies and build
RUN npm install typescript
RUN npm install \
    && npx tsc

# Copy supervisord config
COPY supervisord.conf /etc/supervisord.conf

# Copy entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Expose ports
ARG ENGINE_PORT=21801
ARG WEBSERVER_PORT=8080
EXPOSE ${ENGINE_PORT} ${WEBSERVER_PORT}

# Start entrypoint
CMD ["/entrypoint.sh"]