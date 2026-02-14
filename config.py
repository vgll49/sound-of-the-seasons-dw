# config.py
"""
Central Configuration for Sound of Seasons Data Warehouse
"""
from datetime import date, timedelta

# ==================== DATE RANGES ====================
START_DATE = date(2020, 1, 1)  # Extended to 2020!
END_DATE = date(2026, 2, 14)

# Derived
YEARS = list(range(START_DATE.year, END_DATE.year + 1))
EXPECTED_DAYS = (END_DATE - START_DATE).days + 1

# Chart dates (Sundays only for weekly charts)
def get_first_sunday(start_date):
    """Find first Sunday on or after start_date"""
    current = start_date
    while current.weekday() != 6:  # 6 = Sunday
        current += timedelta(days=1)
    return current

def get_last_sunday(end_date):
    """Find last Sunday on or before end_date"""
    current = end_date
    while current.weekday() != 6:
        current -= timedelta(days=1)
    return current

CHART_START_DATE = get_first_sunday(START_DATE)  # First Sunday >= START_DATE
CHART_END_DATE = get_last_sunday(END_DATE)        # Last Sunday <= END_DATE


# ==================== DATA SOURCES ====================
CHARTS_CSV = "data/raw/soundcharts_charts.csv"
FEATURES_CSV = "data/raw/soundcharts_track_features.csv"

# Database
DATABASE_URL = "sqlite:///sound_of_seasons.db"

# ==================== ETL SETTINGS ====================
BATCH_SIZE_TRACKS = 1000
BATCH_SIZE_FACTS = 5000
BATCH_SIZE_WEATHER = 500

# Chart fetching
CHARTS_BATCH_SIZE = 10  # Weeks per batch
FEATURES_BATCH_SIZE = 50  # Songs per batch

# Weather locations (all 16 Bundesländer)
WEATHER_LOCATIONS = {
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

# ==================== VALIDATION ====================
EXPECTED_DIM_WEATHER_MIN = 300
EXPECTED_DIM_TRACK_MIN = 3000  

# ==================== DISPLAY ====================
DATASET_NAME = "Soundcharts DE Top 200"
DATASET_YEARS = f"{START_DATE.year}-{END_DATE.year}"
DATE_RANGE_STR = f"{START_DATE} → {END_DATE}"