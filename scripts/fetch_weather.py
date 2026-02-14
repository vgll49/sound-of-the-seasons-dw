import asyncio
from services.weather_service import WeatherService
from services.data_loader import DataLoader
from config import START_DATE, END_DATE, BATCH_SIZE_WEATHER
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    logger.info("Fetching weather data...")
    logger.info(f"Period: {START_DATE} → {END_DATE}")
    
    weather_service = WeatherService(
        start_date=START_DATE.isoformat(),
        end_date=END_DATE.isoformat()
    )
    
    loader = DataLoader(batch_size=BATCH_SIZE_WEATHER)
    await loader.load_weather(weather_service)
    
    logger.info("Weather data loaded")

if __name__ == "__main__":
    asyncio.run(main())