"""Database fact linking service"""
from sqlalchemy.orm import Session
from db.models import FactTrackChart, DimWeather
from db.database import SessionLocal
import logging

logger = logging.getLogger(__name__)

class FactLinker:
    """Service for linking fact records to dimensions"""
    
    def __init__(self):
        self.db: Session = SessionLocal()
    
    def link_weather_to_facts(self):
        """Link facts to weather based on date_id"""
        logger.info("Linking facts to weather...")
        
        facts = self.db.query(FactTrackChart).filter(
            FactTrackChart.weather_id.is_(None)
        ).all()
        
        logger.info(f"  Found {len(facts):,} facts to link")
        
        updated = 0
        
        for fact in facts:
            weather = self.db.query(DimWeather).filter(
                DimWeather.date_id == fact.date_id
            ).first()
            
            if weather:
                fact.weather_id = weather.weather_id
                updated += 1
            
            if updated % 5000 == 0 and updated > 0:
                self.db.commit()
                logger.info(f"  Progress: {updated:,}/{len(facts):,}")
        
        self.db.commit()
        logger.info(f"✓ Linked {updated:,}/{len(facts):,} facts to weather ({updated/len(facts)*100:.1f}%)")
        self.db.close()