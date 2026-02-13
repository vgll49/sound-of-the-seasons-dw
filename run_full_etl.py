# run_full_etl.py
"""
Master ETL Script - Sound of Seasons Data Warehouse
"""
import asyncio
import sys
import os
from pathlib import Path

# Setup Python Path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_script(script_path: str, description: str):
    """Führe ein Python-Script aus"""
    logger.info(f"\n{'='*60}")
    logger.info(f"STEP: {description}")
    logger.info(f"{'='*60}")
    
    try:
        # Import und execute
        script_name = Path(script_path).stem
        script_dir = Path(script_path).parent
        
        # Temporär ins Verzeichnis wechseln
        original_dir = os.getcwd()
        os.chdir(project_root)
        
        # if script_dir.name == "scripts":
        #     from scripts import (
        #         create_db, populate_dim_time, 
        #         prepare_charts, load_charts
        #     )
            
        if script_name == "create_db":
            from scripts import create_db
            create_db.create_database()
        elif script_name == "populate_dim_time":
            from scripts import populate_dim_time
            populate_dim_time.populate_dim_time()
        elif script_name == "prepare_charts":
            from scripts import prepare_charts
            prepare_charts.prepare_charts()
        elif script_name == "load_charts":
            from scripts import load_charts
            load_charts.load_tracks_and_facts()
        else:
            raise ValueError(f"Unknown script: {script_name}")

        
        os.chdir(original_dir)
        logger.info(f"✓ {description} completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"✗ {description} failed: {e}")
        return False

async def run_etl():
    """Führe ETL (Weather + Holidays) aus"""
    logger.info(f"\n{'='*60}")
    logger.info(f"STEP: ETL - Fetch Weather & Holiday Data")
    logger.info(f"{'='*60}")
    
    try:
        from etl.fetch_weather import WeatherFetcher
        from etl.fetch_holidays import HolidayFetcher
        from etl.load_data import DataLoader
        from etl.link_facts import FactLinker
        
        start_date = "2020-01-01"
        end_date = "2022-12-31"
        # TODO: Expand this
        year = 2022
        
        loader = DataLoader(batch_size=500)
        
        # Weather
        logger.info("\n→ Fetching Weather Data...")
        weather_fetcher = WeatherFetcher(start_date=start_date, end_date=end_date)
        await loader.load_weather(weather_fetcher)
        
        # Holidays
        logger.info("\n→ Fetching Holiday Data...")
        holiday_fetcher = HolidayFetcher(year=year)
        await loader.load_holidays(holiday_fetcher)
        
        # Link Facts
        logger.info("\n→ Linking Facts to Dimensions...")
        linker = FactLinker()
        linker.link_weather_to_facts()
        linker.link_holidays_to_facts()
        
        logger.info("✓ ETL completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"✗ ETL failed: {e}")
        return False

def validate_data():
    """Validiere die geladenen Daten"""
    logger.info(f"\n{'='*60}")
    logger.info(f"STEP: Data Validation")
    logger.info(f"{'='*60}")
    
    try:
        from db.database import SessionLocal
        from db.models import DimTime, DimTrack, DimWeather, DimHoliday, FactTrackChart
        
        db = SessionLocal()
        
        stats = {
            'DimTime': db.query(DimTime).count(),
            'DimTrack': db.query(DimTrack).count(),
            'DimWeather': db.query(DimWeather).count(),
            'DimHoliday': db.query(DimHoliday).count(),
            'FactTrackChart': db.query(FactTrackChart).count(),
        }
        
        # Facts mit Verknüpfungen
        with_weather = db.query(FactTrackChart).filter(
            FactTrackChart.weather_id.isnot(None)
        ).count()
        
        with_holidays = db.query(FactTrackChart).filter(
            FactTrackChart.holiday_id.isnot(None)
        ).count()
        
        db.close()
        
        logger.info("\n📊 Database Statistics:")
        logger.info(f"  DimTime:        {stats['DimTime']:>8,} rows (expected: 1096)")
        logger.info(f"  DimTrack:       {stats['DimTrack']:>8,} rows")
        logger.info(f"  DimWeather:     {stats['DimWeather']:>8,} rows (expected: ~5,840)")
        logger.info(f"  DimHoliday:     {stats['DimHoliday']:>8,} rows")
        logger.info(f"  FactTrackChart: {stats['FactTrackChart']:>8,} rows")
        logger.info(f"\n🔗 Fact Linkages:")
        logger.info(f"  With Weather:   {with_weather:>8,} ({with_weather/stats['FactTrackChart']*100:.1f}%)")
        logger.info(f"  With Holidays:  {with_holidays:>8,} ({with_holidays/stats['FactTrackChart']*100:.1f}%)")
        
        # Validierung
        errors = []
        if stats['DimTime'] != 365:
            errors.append(f"DimTime should have 365 rows, has {stats['DimTime']}")
        if stats['DimWeather'] < 5000:
            errors.append(f"DimWeather seems incomplete ({stats['DimWeather']} rows)")
        if stats['FactTrackChart'] == 0:
            errors.append("FactTrackChart is empty!")
        
        if errors:
            logger.warning("\n⚠️  Validation Warnings:")
            for error in errors:
                logger.warning(f"  - {error}")
            return False
        
        logger.info("\n✓ All validations passed!")
        return True
        
    except Exception as e:
        logger.error(f"✗ Validation failed: {e}")
        return False

async def main():
    """Hauptfunktion - orchestriert den gesamten ETL-Prozess"""
    start_time = datetime.now()
    
    logger.info("""
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║        🎵 SOUND OF SEASONS - DATA WAREHOUSE ETL 🎵        ║
    ║                                                           ║
    ║           Spotify Charts × Weather × Holidays             ║
    ║                      2022 Dataset                         ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    steps = []
    
    # Step 1: Create Database
    success = run_script(
        "scripts/create_db.py",
        "Create Database Schema"
    )
    steps.append(("Create Database", success))
    if not success:
        logger.error("❌ ETL aborted - Database creation failed")
        return
    
    # Step 2: Populate DimTime
    success = run_script(
        "scripts/populate_dim_time.py",
        "Populate Time Dimension (2022)"
    )
    steps.append(("Populate DimTime", success))
    if not success:
        logger.error("❌ ETL aborted - DimTime population failed")
        return
    
    # Step 3: Prepare Charts
    success = run_script(
        "scripts/prepare_charts.py",
        "Prepare Chart Data (Filter & Clean)"
    )
    steps.append(("Prepare Charts", success))
    if not success:
        logger.error("❌ ETL aborted - Chart preparation failed")
        return
    
    # Step 4: Load Charts
    success = run_script(
        "scripts/load_charts.py",
        "Load Charts into Database"
    )
    steps.append(("Load Charts", success))
    if not success:
        logger.error("❌ ETL aborted - Chart loading failed")
        return
    
    # Step 5: Run ETL (Weather + Holidays + Linking)
    success = await run_etl()
    steps.append(("Fetch & Link Weather/Holidays", success))
    if not success:
        logger.error("❌ ETL aborted - Weather/Holiday ETL failed")
        return
    
    # Step 6: Validate
    success = validate_data()
    steps.append(("Data Validation", success))
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info(f"\n{'='*60}")
    logger.info("📋 ETL SUMMARY")
    logger.info(f"{'='*60}")
    
    for step_name, step_success in steps:
        status = "✓" if step_success else "✗"
        logger.info(f"  {status} {step_name}")
    
    logger.info(f"\n⏱️  Total Duration: {duration.total_seconds():.1f} seconds")
    
    all_success = all(success for _, success in steps)
    
    if all_success:
        logger.info("""
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║              ✅ ETL COMPLETED SUCCESSFULLY! ✅           ║
    ║                                                           ║
    ║     Your data warehouse is ready for analysis! 🎉        ║
    ║                                                           ║
    ║  Next steps:                                              ║
    ║  1. Run queries: python scripts/sample_queries.py         ║
    ║  2. Generate viz: python visualization/generate.py        ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
        """)
    else:
        logger.error("""
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║                ❌ ETL FAILED ❌                          ║
    ║                                                           ║
    ║  Check the logs above for error details.                  ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
        """)

if __name__ == "__main__":
    asyncio.run(main())