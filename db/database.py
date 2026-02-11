from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

DATABASE_URL = "sqlite:///sound_of_seasons.db"

engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True
)

SessionLocal = sessionmaker(bind=engine)
