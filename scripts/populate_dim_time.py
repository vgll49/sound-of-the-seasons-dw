from sqlalchemy.orm import Session
from db.models import DimTime
from db.database import SessionLocal
from datetime import timedelta
from config import START_DATE, END_DATE

def populate_dim_time():
    """Populate DimTime dimension"""
    
    print(f"Populating DimTime: {START_DATE} → {END_DATE}")
    
    db: Session = SessionLocal()
    
    current = START_DATE
    records = []
    
    while current <= END_DATE:
        # Season
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
            month=current.month,
            season=season
        )
        records.append(record)
        current += timedelta(days=1)
    
    try:
        db.bulk_save_objects(records)
        db.commit()
        print(f"Inserted {len(records)} dates ({START_DATE} → {END_DATE})")
        
    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    populate_dim_time()