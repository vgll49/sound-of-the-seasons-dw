"""
Master ETL Orchestrator - Sound of Seasons Data Warehouse
Coordinates all ETL steps in proper sequence
"""
import asyncio
import inspect
import logging
from datetime import datetime
from config import (
    START_DATE, END_DATE, EXPECTED_DAYS,
    DATASET_NAME, DATASET_YEARS, DATE_RANGE_STR,
    EXPECTED_DIM_WEATHER_MIN, EXPECTED_DIM_TRACK_MIN
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_step(step_num: int, name: str, func, *args, **kwargs):
    """Execute a single ETL step"""
    logger.info(f"\n{'='*70}")
    logger.info(f"STEP {step_num}: {name}")
    logger.info(f"{'='*70}")
    
    try:
        if inspect.iscoroutinefunction(func):
            asyncio.run(func(*args, **kwargs))
        else:
            func(*args, **kwargs)
        logger.info(f"Step {step_num} completed: {name}")
        return True
    except Exception as e:
        logger.error(f"Step {step_num} failed: {name}")
        logger.error(f"  Error: {e}")
        import traceback
        traceback.print_exc()
        return False

def validate_data():
    """Validate loaded data"""
    logger.info(f"\n{'='*70}")
    logger.info(f"DATA VALIDATION")
    logger.info(f"{'='*70}")
    
    try:
        from db.database import SessionLocal
        from db.models import DimTime, DimTrack, DimWeather, FactTrackChart
        
        db = SessionLocal()
        
        stats = {
            'DimTime': db.query(DimTime).count(),
            'DimTrack': db.query(DimTrack).count(),
            'DimWeather': db.query(DimWeather).count(),
            'FactTrackChart': db.query(FactTrackChart).count(),
        }
        
        with_weather = db.query(FactTrackChart).filter(
            FactTrackChart.weather_id.isnot(None)
        ).count()
        
        with_features = db.query(DimTrack).filter(
            DimTrack.danceability.isnot(None)
        ).count()
        
        db.close()
        
        logger.info("\nDatabase Statistics:")
        logger.info(f"  DimTime:        {stats['DimTime']:>8,} rows (expected: {EXPECTED_DAYS:,})")
        logger.info(f"  DimTrack:       {stats['DimTrack']:>8,} rows (>={EXPECTED_DIM_TRACK_MIN:,})")
        logger.info(f"    w/ features:  {with_features:>8,} ({with_features/stats['DimTrack']*100:.1f}%)")
        logger.info(f"  DimWeather:     {stats['DimWeather']:>8,} rows (>={EXPECTED_DIM_WEATHER_MIN})")
        logger.info(f"  FactTrackChart: {stats['FactTrackChart']:>8,} rows")
        logger.info(f"\nFact Linkages:")
        logger.info(f"  With Weather:   {with_weather:>8,} ({with_weather/stats['FactTrackChart']*100:.1f}%)")
        
        errors = []
        warnings = []
        
        if stats['DimTime'] != EXPECTED_DAYS:
            errors.append(f"DimTime should have {EXPECTED_DAYS} rows, has {stats['DimTime']}")
        
        if stats['DimWeather'] < EXPECTED_DIM_WEATHER_MIN:
            errors.append(f"DimWeather incomplete ({stats['DimWeather']} rows)")
        
        if stats['FactTrackChart'] == 0:
            errors.append("FactTrackChart is empty")
        
        if stats['DimTrack'] < EXPECTED_DIM_TRACK_MIN:
            warnings.append(f"DimTrack might be incomplete ({stats['DimTrack']} rows)")
        
        if with_features / stats['DimTrack'] < 0.90:
            warnings.append(f"Audio feature coverage low ({with_features/stats['DimTrack']*100:.1f}%)")
        
        if with_weather / stats['FactTrackChart'] < 0.99:
            warnings.append(f"Weather linkage incomplete ({with_weather/stats['FactTrackChart']*100:.1f}%)")
        
        if errors:
            logger.error("\nVALIDATION ERRORS:")
            for error in errors:
                logger.error(f"  - {error}")
            return False
        
        if warnings:
            logger.warning("\nValidation Warnings:")
            for warning in warnings:
                logger.warning(f"  - {warning}")
        
        logger.info("\nAll critical validations passed")
        return True
        
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

async def run_weather_etl():
    """Run weather fetching and loading"""
    import aiohttp
    from services.weather_service import WeatherService
    from services.data_loader import DataLoader
    from config import BATCH_SIZE_WEATHER
    
    async with aiohttp.ClientSession() as session:
        weather_service = WeatherService(
            session=session,
            start_date=START_DATE.isoformat(),
            end_date=END_DATE.isoformat()
        )
        
        loader = DataLoader(batch_size=BATCH_SIZE_WEATHER)
        await loader.load_weather(weather_service)

def main():
    """Main ETL orchestration"""
    start_time = datetime.now()
    
    logger.info(f"""
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║        SOUND OF SEASONS - DATA WAREHOUSE ETL              ║
    ║                                                           ║
    ║           Spotify Charts × Weather × Seasons              ║
    ║              {DATASET_NAME:<40} ║
    ║              Period: {DATE_RANGE_STR:<34} ║
    ║              Years:  {DATASET_YEARS:<34} ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
    """)
    
    steps = []
    
    # Step 1: Create Database
    from scripts.create_db import create_database
    success = run_step(1, "Create Database Schema", create_database)
    steps.append(("Create Database", success))
    if not success:
        return
    
    # Step 2: Populate DimTime
    from scripts.populate_dim_time import populate_dim_time
    success = run_step(2, f"Populate Time Dimension ({DATASET_YEARS})", populate_dim_time)
    steps.append(("Populate DimTime", success))
    if not success:
        return
    
    # Step 3: Fetch & Load Weather
    success = run_step(3, "Fetch & Load Weather Data", run_weather_etl)
    steps.append(("Weather ETL", success))
    if not success:
        return
    
    # Step 4: Load Soundcharts Data
    from scripts.load_soundcharts_data import load_soundcharts_data
    success = run_step(4, "Load Soundcharts Charts + Features", load_soundcharts_data)
    steps.append(("Load Soundcharts Data", success))
    if not success:
        return
    
    # Step 5: Validate
    success = validate_data()
    steps.append(("Data Validation", success))
    
    # Summary
    end_time = datetime.now()
    duration = end_time - start_time
    
    logger.info(f"\n{'='*70}")
    logger.info("ETL SUMMARY")
    logger.info(f"{'='*70}")
    
    for step_name, step_success in steps:
        status = "✓" if step_success else "✗"
        logger.info(f"  {status} {step_name}")
    
    logger.info(f"\nTotal Duration: {duration.total_seconds():.1f} seconds")
    logger.info(f"Duration (min):  {duration.total_seconds()/60:.2f} minutes")
    
    all_success = all(success for _, success in steps)
    
    if all_success:
        logger.info(f"""
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║              ETL COMPLETED SUCCESSFULLY                   ║
    ║                                                           ║
    ║     Your data warehouse is ready for analysis             ║
    ║                                                           ║
    ║  Next steps:                                              ║
    ║  • Generate visualizations:                               ║
    ║    python visualization/generate_dashboard.py             ║
    ║                                                           ║
    ║  Dataset: {DATASET_NAME:<44} ║
    ║  Period:  {DATE_RANGE_STR:<44} ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
        """)
    else:
        logger.error("""
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║                ETL FAILED                                 ║
    ║                                                           ║
    ║  Check the logs above for error details.                  ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
        """)

if __name__ == "__main__":
    main()