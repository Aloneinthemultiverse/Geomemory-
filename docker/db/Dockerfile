# PostgreSQL 16 + PostGIS 3.4 + Apache AGE 1.5 (graph / openCypher) in one image.
# Ubuntu 24.04 ships all three as distro packages, for amd64 and arm64 alike
# (so Apple Silicon runs it natively).
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive \
    PGDATA=/var/lib/postgresql/data \
    PATH=/usr/lib/postgresql/16/bin:$PATH
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      postgresql-16 postgresql-16-postgis-3 postgresql-16-age \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p "$PGDATA" /var/run/postgresql \
 && chown -R postgres:postgres /var/lib/postgresql /var/run/postgresql
COPY entrypoint.sh /usr/local/bin/geomemory-db
COPY init.sql /docker-entrypoint-initdb.d/init.sql
RUN chmod +x /usr/local/bin/geomemory-db
USER postgres
EXPOSE 5432
VOLUME /var/lib/postgresql/data
ENTRYPOINT ["geomemory-db"]
