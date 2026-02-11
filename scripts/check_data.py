# scripts/check_data.py
import sys
import os

from db.database import SessionLocal
from db.models import DimTime, DimTrack, DimWeather, DimHoliday, FactTrackChart

db = SessionLocal()

print("\n=== Database Statistics ===")
print(f"DimTime:       {db.query(DimTime).count()} rows (expected: 365)")
print(f"DimTrack:      {db.query(DimTrack).count()} rows")
print(f"DimWeather:    {db.query(DimWeather).count()} rows (expected: ~5,840)")
print(f"DimHoliday:    {db.query(DimHoliday).count()} rows")
print(f"FactTrackChart: {db.query(FactTrackChart).count()} rows")

# Mit Weather verknüpft?
with_weather = db.query(FactTrackChart).filter(
    FactTrackChart.weather_id.isnot(None)
).count()
print(f"\nFacts with weather: {with_weather}")

# Mit Holidays verknüpft?
with_holidays = db.query(FactTrackChart).filter(
    FactTrackChart.holiday_id.isnot(None)
).count()
print(f"Facts with holidays: {with_holidays}")

db.close()