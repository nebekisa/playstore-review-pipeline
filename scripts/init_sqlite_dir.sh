#!/bin/sh
# scripts/init_sqlite_dir.sh

set -e # Exit immediately if a command exits with a non-zero status.

echo "Starting SQLite directory initialization..."

# Create the directory if it doesn't exist
mkdir -p /app/database

# Change ownership to Airflow user (50000:50000)
chown -R 50000:50000 /app/database

# Set directory permissions (rwx for owner, rx for group/others)
chmod -R 755 /app/database

# Create the database file
touch /app/database/reviews.db

# Change ownership of the database file
chown 50000:50000 /app/database/reviews.db

# Set file permissions (rw for owner, r for group/others)
chmod 644 /app/database/reviews.db

echo "SQLite directory (/app/database) and file (reviews.db) initialized with correct permissions for user 50000:50000."
exit 0