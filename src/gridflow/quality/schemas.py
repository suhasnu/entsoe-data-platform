from pandera.pandas import Check, Column, DataFrameSchema

ZONE = Column(str, Check.str_matches(r"^[A-Z]{2}(_[A-Z0-9]+)?$"), nullable=False)
TIMESTAMP = Column("datetime64[ns, UTC]", nullable=False)
RUN_ID = Column(str, nullable=False)

# Zones publish at 15 or 60 minutes. Anything else means the source changed and
# the hourly conforming downstream would silently produce nonsense.
RESOLUTION = Column(int, Check.isin([15, 60]), nullable=False)

# German peak load is around 80 GW. Loose enough for any EU zone, tight enough
# to catch a unit error.
LOAD_SCHEMA = DataFrameSchema(
    {
        "zone_code": ZONE,
        "ts_utc": TIMESTAMP,
        "load_mw": Column(float, Check.in_range(0, 150_000), nullable=True),
        "resolution_minutes": RESOLUTION,
        "run_id": RUN_ID,
    },
    unique=["zone_code", "ts_utc"],
    strict="filter",
    coerce=True,
)

GENERATION_SCHEMA = DataFrameSchema(
    {
        "zone_code": ZONE,
        "ts_utc": TIMESTAMP,
        "production_type": Column(str, nullable=False),
        "generation_mw": Column(float, Check.in_range(0, 150_000), nullable=True),
        "resolution_minutes": RESOLUTION,
        "run_id": RUN_ID,
    },
    unique=["zone_code", "ts_utc", "production_type"],
    strict="filter",
    coerce=True,
)

# Negative prices are normal in Germany during renewable surplus, so the floor
# is the exchange cap rather than zero.
PRICE_SCHEMA = DataFrameSchema(
    {
        "zone_code": ZONE,
        "ts_utc": TIMESTAMP,
        "price_eur_mwh": Column(float, Check.in_range(-500, 4_000), nullable=True),
        "resolution_minutes": RESOLUTION,
        "run_id": RUN_ID,
    },
    unique=["zone_code", "ts_utc"],
    strict="filter",
    coerce=True,
)
