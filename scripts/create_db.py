# scripts/01_create_db.py

from db.database import engine
from db.models import Base

def create_database():
    """Erstellt alle Tabellen"""
    Base.metadata.create_all(bind=engine)
    print("✓ Database tables created")

if __name__ == "__main__":
    create_database()