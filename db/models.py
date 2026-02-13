# db/models.py
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Boolean,
    Date, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class DimTime(Base):
    __tablename__ = "dim_time"

    date_id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False, unique=True)

    day = Column(Integer)
    month = Column(Integer)
    year = Column(Integer)
    weekday = Column(Integer)
    week_of_year = Column(Integer)
    season = Column(String)
    is_weekend = Column(Boolean)

class DimTrack(Base):
    __tablename__ = "dim_track"

    track_id = Column(String, primary_key=True)
    track_name = Column(String, nullable=False)
    artist_names = Column(String)
    genre = Column(String)

    duration_ms = Column(Integer)
    explicit_flag = Column(Boolean)
    popularity = Column(Integer)  # 0-100
    release_date = Column(String)  # YYYY oder YYYY-MM-DD
    
    # Audio Features (alle von Spotify)
    danceability = Column(Float)       # 0.0-1.0
    energy = Column(Float)             # 0.0-1.0
    valence = Column(Float)            # 0.0-1.0 (Positivität)
    tempo = Column(Float)              # BPM
    loudness = Column(Float)           # dB (-60 to 0)
    speechiness = Column(Float)        # 0.0-1.0
    acousticness = Column(Float)       # 0.0-1.0
    instrumentalness = Column(Float)   # 0.0-1.0
    liveness = Column(Float)           # 0.0-1.0
    key = Column(Integer)              # 0-11 (C, C#, D, ...)
    mode = Column(Integer)             # 0=minor, 1=major
    time_signature = Column(Integer)   # Beats per bar (3, 4, 5, ...)

class DimWeather(Base):
    __tablename__ = "dim_weather"

    weather_id = Column(Integer, primary_key=True)
    date_id = Column(Integer, ForeignKey("dim_time.date_id"), nullable=False)

    bundesland = Column(String, nullable=False)

    temperature_avg = Column(Float)
    precipitation_mm = Column(Float)
    wind_speed_kmh = Column(Float)
    sunshine_hours = Column(Float)

    time = relationship("DimTime")

class DimHoliday(Base):
    __tablename__ = "dim_holiday"

    holiday_id = Column(Integer, primary_key=True)
    date_id = Column(Integer, ForeignKey("dim_time.date_id"), nullable=False)

    bundesland = Column(String, nullable=False)
    holiday_name = Column(String)
    is_public_holiday = Column(Boolean)

    time = relationship("DimTime")

class FactTrackChart(Base):
    __tablename__ = "fact_track_chart"

    fact_id = Column(Integer, primary_key=True)

    track_id = Column(String, ForeignKey("dim_track.track_id"), nullable=False)
    date_id = Column(Integer, ForeignKey("dim_time.date_id"), nullable=False)
    weather_id = Column(Integer, ForeignKey("dim_weather.weather_id"))
    holiday_id = Column(Integer, ForeignKey("dim_holiday.holiday_id"))

    country = Column(String, nullable=False) 

    stream_count = Column(Integer)
    chart_position = Column(Integer)

    track = relationship("DimTrack")
    time = relationship("DimTime")
    weather = relationship("DimWeather")
    holiday = relationship("DimHoliday")