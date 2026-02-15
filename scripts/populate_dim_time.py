from sqlalchemy.orm import Session
from db.models import DimTime
from db.database import SessionLocal
from datetime import timedelta, date
from config import START_DATE, END_DATE
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def populate_dim_time(start_date=None, end_date=None):
    """
    Populate DimTime dimension
    
    Args:
        start_date: Start date (default: config.START_DATE)
        end_date: End date (default: config.END_DATE)
    
    Usage:
        # Initial load (uses config)
        populate_dim_time()
        
        # Extend to today
        populate_dim_time(start_date=latest_date + 1, end_date=date.today())
    """
    
    # Use config defaults if not provided
    start = start_date or START_DATE
    end = end_date or END_DATE
    
    logger.info(f"Populating DimTime: {start} → {end}")
    
    db: Session = SessionLocal()
    
    try:
        # Check what already exists
        existing_dates = {d.date for d in db.query(DimTime.date).all()}
        
        if existing_dates:
            logger.info(f"  {len(existing_dates)} dates already in DimTime")
        
        # Generate records
        current = start
        records = []
        skipped = 0
        
        while current <= end:
            # Skip if already exists
            if current in existing_dates:
                skipped += 1
                current += timedelta(days=1)
                continue
            
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
        
        # Insert new records
        if records:
            db.bulk_save_objects(records)
            db.commit()
            logger.info(f"Inserted {len(records)} new dates")
        
        if skipped > 0:
            logger.info(f"Skipped {skipped} existing dates")
        
        if not records and not skipped:
            logger.info("No dates to insert")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error: {e}")
        raise
    finally:
        db.close()

if __name__ == "__main__":
    populate_dim_time()