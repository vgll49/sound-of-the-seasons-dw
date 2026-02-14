"""Database loading service"""
from typing import Dict
from sqlalchemy.orm import Session
from db.models import DimTime, DimWeather
from db.database import SessionLocal
import logging

logger = logging.getLogger(__name__)

class DataLoader:
    """Service for loading data into database"""
    
    def __init__(self, batch_size: int = 500):
        self.batch_size = batch_size
    
    async def load_weather(self, weather_service):
        """Load averaged weather data for Germany"""
        db: Session = SessionLocal()
        batch = []
        total = 0
        
        date_lookup = self._get_date_lookup(db)
        
        try:
            async for record in weather_service.fetch_all():
                date_id = date_lookup.get(record["date"])
                if not date_id:
                    logger.warning(f"Date {record['date']} not in dim_time, skipping")
                    continue
                
                weather = DimWeather(
                    date_id=date_id,
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
            
            if batch:
                db.bulk_save_objects(batch)
                db.commit()
                total += len(batch)
            
            logger.info(f"✓ Total weather records inserted: {total}")
            
        finally:
            db.close()
    
    def _get_date_lookup(self, db: Session) -> Dict[str, int]:
        """Create lookup dict: date_string -> date_id"""
        dates = db.query(DimTime.date_id, DimTime.date).all()
        return {d.date.isoformat(): d.date_id for d in dates}