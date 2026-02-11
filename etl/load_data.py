# etl/load_data.py
from typing import Dict
from sqlalchemy.orm import Session
from db.models import DimTime, DimWeather, DimHoliday
from db.database import SessionLocal
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, batch_size: int = 500):
        self.batch_size = batch_size
    
    async def load_weather(self, fetcher):
        """Load weather data incrementally"""
        db: Session = SessionLocal()
        batch = []
        total = 0
        
        # Create date lookup
        date_lookup = self._get_date_lookup(db)
        
        try:
            async for record in fetcher.fetch_all():
                date_id = date_lookup.get(record["date"])
                if not date_id:
                    logger.warning(f"Date {record['date']} not in dim_time, skipping")
                    continue
                
                weather = DimWeather(
                    date_id=date_id,
                    bundesland=record["bundesland"],
                    temperature_avg=record["temperature_avg"],
                    precipitation_mm=record["precipitation_mm"],
                    wind_speed_kmh=record["wind_speed_kmh"],
                    sunshine_hours=record["sunshine_hours"]
                )
                batch.append(weather)
                
                if len(batch) >= self.batch_size:
                    db.bulk_save_objects(batch)
                    db.commit()
                    total += len(batch)
                    logger.info(f"Inserted {total} weather records")
                    batch = []
            
            # Insert remaining
            if batch:
                db.bulk_save_objects(batch)
                db.commit()
                total += len(batch)
            
            logger.info(f"✓ Total weather records inserted: {total}")
            
        finally:
            db.close()
    
    async def load_holidays(self, fetcher):
        """Load holiday data incrementally"""
        db: Session = SessionLocal()
        batch = []
        total = 0
        
        date_lookup = self._get_date_lookup(db)
        
        try:
            async for record in fetcher.fetch_all():
                date_id = date_lookup.get(record["date"])
                if not date_id:
                    continue
                
                holiday = DimHoliday(
                    date_id=date_id,
                    bundesland=record["bundesland"],
                    holiday_name=record["holiday_name"],
                    is_public_holiday=record["is_public_holiday"]
                )
                batch.append(holiday)
                
                if len(batch) >= self.batch_size:
                    db.bulk_save_objects(batch)
                    db.commit()
                    total += len(batch)
                    logger.info(f"Inserted {total} holiday records")
                    batch = []
            
            if batch:
                db.bulk_save_objects(batch)
                db.commit()
                total += len(batch)
            
            logger.info(f"✓ Total holiday records inserted: {total}")
            
        finally:
            db.close()
    
    def _get_date_lookup(self, db: Session) -> Dict[str, int]:
        """Create lookup dict: date_string -> date_id"""
        dates = db.query(DimTime.date_id, DimTime.date).all()
        return {d.date.isoformat(): d.date_id for d in dates}