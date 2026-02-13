# scripts/populate_dim_time.py
from sqlalchemy.orm import Session
from db.models import DimTime
from db.database import SessionLocal
from datetime import datetime, timedelta

def populate_dim_time():
    db: Session = SessionLocal()
    
    start_date = datetime(2020, 1, 1).date()
    end_date = datetime(2022, 12, 31).date()
    
    current = start_date
    records = []
    
    while current <= end_date:
        # Saison berechnen
        month = current.month
        if month in [12, 1, 2]:
            season = "Winter"
        elif month in [3, 4, 5]:
            season = "Frühling"
        elif month in [6, 7, 8]:
            season = "Sommer"
        else:
            season = "Herbst"
        
        record = DimTime(
            date=current,
            day=current.day,
            month=current.month,
            year=current.year,
            weekday=current.weekday(),  # 0=Monday, 6=Sunday
            week_of_year=current.isocalendar()[1],
            season=season,
            is_weekend=(current.weekday() >= 5)  # Sa/So
        )
        records.append(record)
        current += timedelta(days=1)
    
    try:
        db.bulk_save_objects(records)
        db.commit()
        print(f"✓ Inserted {len(records)} dates for start date: {start_date} to end date: {end_date}")
    except Exception as e:
        db.rollback()
        print(f"✗ Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    populate_dim_time()

