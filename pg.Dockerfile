FROM postgres:13.4

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=10 \
    CMD PGPASSWORD=${POSTGRES_PASSWORD} psql -h localhost -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "SELECT version()"