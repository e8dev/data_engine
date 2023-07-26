CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS events (
  id            UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
  event_type    TEXT        NOT NULL,
  coordinates   TEXT        NOT NULL,
  device_type   TEXT        NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS results (
  country       TEXT        NOT NULL,
  device_type   TEXT        NOT NULL,
  date          DATE        NOT NULL,
  purchases     BIGINT      NOT NULL,

  PRIMARY KEY (country, device_type, date)
);

COPY events(id, created_at, event_type, coordinates, device_type) FROM '/docker-entrypoint-initdb.d/data.csv' DELIMITER ',' CSV;
