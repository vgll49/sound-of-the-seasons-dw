# etl/link_facts.py
from sqlalchemy.orm import Session
from sqlalchemy import and_
from db.models import FactTrackChart, DimWeather, DimHoliday, DimTime
from db.database import SessionLocal
import logging

logger = logging.getLogger(__name__)

class FactLinker:
    def __init__(self):
        self.db: Session = SessionLocal()
    
    def link_weather_to_facts(self):
        """Verknüpfe Facts mit Weather basierend auf date_id"""
        # Für jedes Bundesland - nehmen wir mal Berlin als Standard
        logger.info("Linking facts to weather (using Berlin data)...")
        
        facts = self.db.query(FactTrackChart).filter(
            FactTrackChart.weather_id.is_(None)
        ).all()
        
        updated = 0
        for fact in facts:
            # Suche Weather-Eintrag für Berlin am selben Datum
            weather = self.db.query(DimWeather).filter(
                and_(
                    DimWeather.date_id == fact.date_id,
                    DimWeather.bundesland == "Berlin"  # Oder avg über alle
                )
            ).first()
            
            if weather:
                fact.weather_id = weather.weather_id
                updated += 1
        
        self.db.commit()
        logger.info(f"✓ Linked {updated} facts to weather")
    
    def link_holidays_to_facts(self):
        """Verknüpfe Facts mit Holidays"""
        logger.info("Linking facts to holidays...")
        
        facts = self.db.query(FactTrackChart).filter(
            FactTrackChart.holiday_id.is_(None)
        ).all()
        
        updated = 0
        for fact in facts:
            # Prüfe ob es einen bundesweiten Feiertag gibt
            holiday = self.db.query(DimHoliday).filter(
                DimHoliday.date_id == fact.date_id
            ).first()  # Nimm ersten gefundenen
            
            if holiday:
                fact.holiday_id = holiday.holiday_id
                updated += 1
        
        self.db.commit()
        logger.info(f"✓ Linked {updated} facts to holidays")
    
    def __del__(self):
        self.db.close()