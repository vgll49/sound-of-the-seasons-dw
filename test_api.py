# test_api.py
import asyncio
from etl.fetch_weather import WeatherFetcher
from etl.fetch_holidays import HolidayFetcher

async def test_weather():
    print("\n=== Testing Weather API (Sequential) ===")
    
    fetcher = WeatherFetcher(
        start_date="2022-01-01",
        end_date="2022-01-07"
    )
    
    count = 0
    async for record in fetcher.fetch_all():
        if count < 3:
            print(f"  {record}")
        count += 1
    
    print(f"\n✓ Total: {count} records (expected: 112 = 16 states × 7 days)")

asyncio.run(test_weather())

async def test_holidays():
    print("\n=== Testing Holiday API ===")
    
    fetcher = HolidayFetcher(year=2023)
    
    count = 0
    async for record in fetcher.fetch_all():
        if count < 5:  # Zeige erste 5
            print(f"  {record}")
        count += 1
    
    print(f"\n✓ Total records fetched: {count}")

async def main():
    await test_weather()
    await test_holidays()

if __name__ == "__main__":
    asyncio.run(main())