import aiohttp
import asyncio
from typing import AsyncGenerator, Dict, List
from collections import defaultdict
import logging
from config import WEATHER_LOCATIONS

logger = logging.getLogger(__name__)

class WeatherService:
    """Fetch weather and compute daily averages"""
    
    BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
    
    def __init__(self, session: aiohttp.ClientSession, start_date: str, end_date: str):
        self.session = session  
        self.start_date = start_date
        self.end_date = end_date
        self.locations = WEATHER_LOCATIONS
    
    async def fetch_location_weather(self, name: str, lat: float, lon: float) -> List[Dict]:
        """Fetch weather data for one location"""
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
                async with self.session.get( 
                    self.BASE_URL, 
                    params=params, 
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    
                    if response.status == 429:
                        wait_time = (2 ** attempt) * 2
                        logger.warning(f"Rate limited for {name}, waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue
                    
                    response.raise_for_status()
                    data = await response.json()
                    
                    daily = data.get("daily", {})
                    dates = daily.get("time", [])
                    temps = daily.get("temperature_2m_mean", [])
                    precips = daily.get("precipitation_sum", [])
                    winds = daily.get("windspeed_10m_max", [])
                    sunshine = daily.get("sunshine_duration", [])
                    
                    records = []
                    for i, date in enumerate(dates):
                        records.append({
                            "date": date,
                            "location": name,
                            "temperature_avg": temps[i],
                            "precipitation_mm": precips[i],
                            "wind_speed_kmh": winds[i],
                            "sunshine_hours": sunshine[i] / 3600 if sunshine[i] else None
                        })
                    
                    logger.info(f"{name}: {len(records)} days")
                    return records
                    
            except Exception as e:
                logger.warning(f"{name} attempt {attempt+1}: {e}")
                if attempt < max_retries - 1:
                    await asyncio.sleep(2)
        
        logger.error(f"Failed: {name}")
        return []
    
    async def fetch_all(self) -> AsyncGenerator[Dict, None]:
        """Fetch all locations and yield AVERAGED daily values"""
        logger.info(f"Fetching weather for {len(self.locations)} locations...")
        
        all_location_data = []
        
        for name, (lat, lon) in self.locations.items():
            records = await self.fetch_location_weather(name, lat, lon)
            all_location_data.append(records)
            await asyncio.sleep(3)
        
        logger.info(f"Fetched {len(self.locations)} locations")
        logger.info("Computing daily averages across Germany...")
        
        daily_averages = self._compute_daily_averages(all_location_data)
        logger.info(f"Computed {len(daily_averages)} daily averages")
        
        for record in daily_averages:
            yield record
    
    def _compute_daily_averages(self, all_location_data: List[List[Dict]]) -> List[Dict]:
        """Average weather data across all locations per day"""
        daily_values = defaultdict(lambda: {
            'temps': [], 'precips': [], 'winds': [], 'sunshines': []
        })
        
        for location_records in all_location_data:
            for record in location_records:
                date = record['date']
                daily_values[date]['temps'].append(record['temperature_avg'])
                daily_values[date]['precips'].append(record['precipitation_mm'])
                daily_values[date]['winds'].append(record['wind_speed_kmh'])
                
                if record['sunshine_hours'] is not None:
                    daily_values[date]['sunshines'].append(record['sunshine_hours'])
        
        averaged = []
        for date in sorted(daily_values.keys()):
            values = daily_values[date]
            
            averaged.append({
                'date': date,
                'temperature_avg': sum(values['temps']) / len(values['temps']) if values['temps'] else None,
                'precipitation_mm': sum(values['precips']) / len(values['precips']) if values['precips'] else None,
                'wind_speed_kmh': sum(values['winds']) / len(values['winds']) if values['winds'] else None,
                'sunshine_hours': sum(values['sunshines']) / len(values['sunshines']) if values['sunshines'] else None
            })
        
        return averaged