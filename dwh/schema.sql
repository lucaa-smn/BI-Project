CREATE TABLE dim_date (
    date_id        integer PRIMARY KEY,   -- 20230115
    full_date      date    NOT NULL,
    year           integer NOT NULL,
    month          integer NOT NULL,
    day            integer NOT NULL,
    day_of_week    integer NOT NULL,     -- 1=Montag ... 7=Sonntag
    day_name       varchar(10) NOT NULL, -- 'Mon', 'Tue', ...
    week_of_year   integer NOT NULL,
    is_weekend     boolean NOT NULL
);

CREATE TABLE dim_airport (
    airport_id  varchar(50) PRIMARY KEY,  -- IATA-Code aus airports_geolocation
    name        text,
    city        text,
    state       text,
    country     text,
    latitude    numeric(9,6),
    longitude   numeric(9,6)
);

CREATE TABLE dim_airline (
    airline_id   varchar(10) PRIMARY KEY, -- Airline-Code aus US_flights_2023
    airline_name text
);

CREATE TABLE dim_weather (
    weather_id   bigserial PRIMARY KEY,
    airport_id   varchar(10) NOT NULL REFERENCES dim_airport (airport_id),
    date_id      integer     NOT NULL REFERENCES dim_date (date_id),

    -- Wettermetriken aus weather_meteo_by_airport.csv
    tavg         numeric(5,2),  -- Durchschnittstemperatur
    prcp         numeric(6,2),  -- Niederschlag
    wspd         numeric(5,2),  -- Windgeschwindigkeit
    -- ggf. weitere Felder: tmin, tmax, snow, ...

    -- Damit jeder Flughafen/Tag nur einmal vorkommt:
    UNIQUE (airport_id, date_id)
);

CREATE TABLE fact_flights (
    flight_id         bigserial PRIMARY KEY,

    -- Fremdschlüssel
    flight_date_id    integer     NOT NULL REFERENCES dim_date (date_id),
    dep_airport_id    varchar(10) NOT NULL REFERENCES dim_airport (airport_id),
    arr_airport_id    varchar(10)     NULL REFERENCES dim_airport (airport_id),
    airline_id        varchar(50) NOT NULL REFERENCES dim_airline (airline_id),
    weather_id        bigint          NULL REFERENCES dim_weather (weather_id),

    -- Business Keys / Identifikation
    flight_number     varchar(20),    -- z. B. "DL1234"
    tail_number       varchar(20),    -- aus Flights/Cancellation File
    sched_dep_time    time,           -- geplante Abflugzeit
    sched_arr_time    time,           -- geplante Ankunftszeit
    dep_time_label    varchar(10),    -- dein normalisiertes Label (z. B. '08-09h')

    -- Measures
    dep_delay_min     numeric(6,2),
    arr_delay_min     numeric(6,2),
    distance          numeric(7,1),

    cancelled         boolean NOT NULL DEFAULT false,
    diverted          boolean NOT NULL DEFAULT false,
    cancellation_code varchar(5),

    -- Abgeleitete Flags für BI/ML
    is_delayed_15     boolean NOT NULL,  -- im ETL setzen: dep_delay_min >= 15

    -- optionale Detail-Delay-Spalten, falls im Datensatz vorhanden:
    carrier_delay_min numeric(6,2),
    weather_delay_min numeric(6,2),
    nas_delay_min     numeric(6,2),
    security_delay_min numeric(6,2),
    late_aircraft_delay_min numeric(6,2)
);


CREATE INDEX idx_fact_flights_date_airport_airline
    ON fact_flights (flight_date_id, dep_airport_id, airline_id);

CREATE INDEX idx_fact_flights_airline
    ON fact_flights (airline_id);

CREATE INDEX idx_fact_flights_cancelled_diverted
    ON fact_flights (cancelled, diverted);
