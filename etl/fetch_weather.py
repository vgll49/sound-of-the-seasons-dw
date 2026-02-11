# etl/fetch_weather.py
import aiohttp
import asyncio
from typing import AsyncGenerator, Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WeatherFetcher:
    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
    
    BUNDESLAENDER = {
        "Baden-Württemberg": (48.7758, 9.1829),
        "Bayern": (48.1351, 11.5820),
        "Berlin": (52.5200, 13.4050),
        "Brandenburg": (52.4125, 12.5316),
        "Bremen": (53.0793, 8.8017),
        "Hamburg": (53.5511, 9.9937),
        "Hessen": (50.1109, 8.6821),
        "Mecklenburg-Vorpommern": (53.6355, 12.6925),
        "Niedersachsen": (52.3759, 9.7320),
        "Nordrhein-Westfalen": (51.4556, 7.0116),
        "Rheinland-Pfalz": (49.9929, 8.2473),
        "Saarland": (49.2401, 6.9969),
        "Sachsen": (51.0504, 13.7373),
        "Sachsen-Anhalt": (51.4969, 11.9688),
        "Schleswig-Holstein": (54.3233, 10.1228),
        "Thüringen": (50.9848, 11.0299)
    }
    
    def __init__(self, start_date: str, end_date: str):
        self.start_date = start_date
        self.end_date = end_date
    
    async def fetch_weather_data(
        self, 
        session: aiohttp.ClientSession,
        bundesland: str,
        lat: float,
        lon: float
    ) -> List[Dict]:
        """Fetch weather data for one Bundesland with retry logic"""
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "daily": "temperature_2m_mean,precipitation_sum,windspeed_10m_max,sunshine_duration",
            "timezone": "Europe/Berlin"
        }
        
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                async with session.get(
                    self.BASE_URL, 
                    params=params, 
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    
                    # Rate Limit handling
                    if response.status == 429:
                        wait_time = (2 ** attempt) * 2  # 2s, 4s, 8s
                        logger.warning(f"⏳ Rate limited for {bundesland}, waiting {wait_time}s (attempt {attempt+1}/{max_retries})")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Transform to records
                    daily = data.get("daily", {})
                    dates = daily.get("time", [])
                    
                    records = []
                    for i, date in enumerate(dates):
                        records.append({
                            "date": date,
                            "bundesland": bundesland,
                            "temperature_avg": daily["temperature_2m_mean"][i],
                            "precipitation_mm": daily["precipitation_sum"][i],
                            "wind_speed_kmh": daily["windspeed_10m_max"][i],
                            "sunshine_hours": daily["sunshine_duration"][i] / 3600 if daily["sunshine_duration"][i] else None
                        })
                    
                    logger.info(f"✓ Fetched weather for {bundesland}: {len(records)} days")
                    return records
                    
            except asyncio.TimeoutError:
                logger.warning(f"⏱️  Timeout for {bundesland} (attempt {attempt+1}/{max_retries})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                    
            except aiohttp.ClientError as e:
                logger.warning(f"🌐 Network error for {bundesland}: {e} (attempt {attempt+1}/{max_retries})")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
                    
            except Exception as e:
                logger.error(f"✗ Unexpected error for {bundesland}: {e}")
                break
        
        logger.error(f"✗ Failed to fetch {bundesland} after {max_retries} attempts")
        return []
    
    async def fetch_all(self) -> AsyncGenerator[Dict, None]:
        """
        Fetch weather data SEQUENTIALLY to avoid rate limits.
        Slower but 100% reliable.
        """
        async with aiohttp.ClientSession() as session:
            for bundesland, (lat, lon) in self.BUNDESLAENDER.items():
                # Fetch one by one
                records = await self.fetch_weather_data(session, bundesland, lat, lon)
                
                for record in records:
                    yield record
                
                # Pause zwischen Bundesländern
                await asyncio.sleep(1)  # 1 Sekunde Pause