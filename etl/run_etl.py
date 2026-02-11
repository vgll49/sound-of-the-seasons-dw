# etl/run_etl.py
import asyncio
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fetch_weather import WeatherFetcher
from fetch_holidays import HolidayFetcher
from load_data import DataLoader
from link_facts import FactLinker  # <- NEU
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def main():
    start_date = "2022-01-01"
    end_date = "2022-12-31"
    year = 2022
    
    loader = DataLoader(batch_size=500)
    
    # 1. Weather
    print("\n=== Fetching Weather Data for 2022 ===")
    weather_fetcher = WeatherFetcher(start_date=start_date, end_date=end_date)
    await loader.load_weather(weather_fetcher)
    
    # 2. Holidays
    print("\n=== Fetching Holiday Data for 2022 ===")
    holiday_fetcher = HolidayFetcher(year=year)
    await loader.load_holidays(holiday_fetcher)
    
    # 3. Link Facts to Weather/Holidays
    print("\n=== Linking Facts to Weather/Holidays ===")
    linker = FactLinker()
    linker.link_weather_to_facts()
    linker.link_holidays_to_facts()
    
    print("\n✓ ETL Complete for 2022!")

if __name__ == "__main__":
    asyncio.run(main())