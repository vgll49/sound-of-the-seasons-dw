from db.database import engine
from db.models import Base
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_database():
    """Create all database tables"""
    logger.info("Creating database schema...")
    Base.metadata.create_all(bind=engine)
    logger.info("✓ Database tables created")

if __name__ == "__main__":
    create_database()