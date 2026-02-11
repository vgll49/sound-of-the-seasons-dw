import aiohttp
import asyncio
from datetime import datetime, timedelta
from typing import AsyncGenerator, Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HolidayFetcher:
    BASE_URL = "https://ferien-api.de/api/v1/holidays"
    
    BUNDESLAENDER_CODES = {
        "BW": "Baden-Württemberg",
        "BY": "Bayern",
        "BE": "Berlin",
        "BB": "Brandenburg",
        "HB": "Bremen",
        "HH": "Hamburg",
        "HE": "Hessen",
        "MV": "Mecklenburg-Vorpommern",
        "NI": "Niedersachsen",
        "NW": "Nordrhein-Westfalen",
        "RP": "Rheinland-Pfalz",
        "SL": "Saarland",
        "SN": "Sachsen",
        "ST": "Sachsen-Anhalt",
        "SH": "Schleswig-Holstein",
        "TH": "Thüringen"
    }
    
    def __init__(self, year: int):
        self.year = year
    
    async def fetch_holidays(
        self,
        session: aiohttp.ClientSession,
        state_code: str
    ) -> List[Dict]:
        """Fetch holidays for one Bundesland"""
        url = f"{self.BASE_URL}/{state_code}/{self.year}"
        
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                data = await response.json()
                
                records = []
                for holiday in data:
                    # Parse date range
                    start = datetime.fromisoformat(holiday["start"])
                    end = datetime.fromisoformat(holiday["end"])
                    
                    # Create record for each day
                    current = start
                    while current <= end:
                        records.append({
                            "date": current.date().isoformat(),
                            "bundesland": self.BUNDESLAENDER_CODES[state_code],
                            "holiday_name": holiday["name"],
                            "is_public_holiday": True
                        })
                        current += timedelta(days=1)
                
                logger.info(f"✓ Fetched holidays for {state_code}: {len(records)} days")
                return records
                
        except Exception as e:
            logger.error(f"✗ Error fetching holidays {state_code}: {e}")
            return []
    
    async def fetch_all(self) -> AsyncGenerator[Dict, None]:
        """Generator that yields holiday records"""
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.fetch_holidays(session, code)
                for code in self.BUNDESLAENDER_CODES.keys()
            ]
            
            for coro in asyncio.as_completed(tasks):
                records = await coro
                for record in records:
                    yield record