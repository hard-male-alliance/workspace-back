#!/usr/bin/env bash

set -Eeuo pipefail

: "${AIWS_DB_MIGRATOR_PASSWORD:?AIWS_DB_MIGRATOR_PASSWORD is required}"
: "${AIWS_DB_APP_PASSWORD:?AIWS_DB_APP_PASSWORD is required}"
: "${AIWS_DB_DASHBOARD_PASSWORD:?AIWS_DB_DASHBOARD_PASSWORD is required}"

if [[ "$AIWS_DB_MIGRATOR_PASSWORD" == "$AIWS_DB_APP_PASSWORD" \
  || "$AIWS_DB_MIGRATOR_PASSWORD" == "$AIWS_DB_DASHBOARD_PASSWORD" \
  || "$AIWS_DB_APP_PASSWORD" == "$AIWS_DB_DASHBOARD_PASSWORD" ]]; then
  printf 'AIWS database role passwords must be distinct.\n' >&2
  exit 1
fi

psql --set ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname postgres <<'SQL'
\getenv migrator_password AIWS_DB_MIGRATOR_PASSWORD
\getenv app_password AIWS_DB_APP_PASSWORD
\getenv dashboard_password AIWS_DB_DASHBOARD_PASSWORD
CREATE ROLE workspace_owner
  NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE workspace_migrator
  LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  PASSWORD :'migrator_password';
CREATE ROLE workspace_app
  LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  PASSWORD :'app_password';
CREATE ROLE workspace_dashboard
  LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
  PASSWORD :'dashboard_password';
GRANT workspace_owner TO workspace_migrator;
ALTER DATABASE ai_job_workspace OWNER TO workspace_owner;
REVOKE ALL ON DATABASE ai_job_workspace FROM PUBLIC;
REVOKE CREATE, TEMPORARY ON DATABASE ai_job_workspace
  FROM workspace_migrator, workspace_app, workspace_dashboard;
GRANT CONNECT ON DATABASE ai_job_workspace
  TO workspace_migrator, workspace_app, workspace_dashboard;
SQL

psql --set ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname ai_job_workspace <<'SQL'
CREATE EXTENSION IF NOT EXISTS vector;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public
  FROM workspace_migrator, workspace_app, workspace_dashboard;
SQL
