from db.database import engine
from db.database import SessionLocal
from datetime import date
from db.models import Base
from sqlalchemy import inspect
from db.models import DimTime, DimTrack

print("Creating tables...")
Base.metadata.create_all(bind=engine)
print("Done.")


inspector = inspect(engine)
print(inspector.get_table_names())

with SessionLocal() as session:
    t = DimTime(
        date_id=20240101,
        date=date(2024, 1, 1),
        day=1,
        month=1,
        year=2024,
        weekday=0,
        week_of_year=1,
        season="winter",
        is_weekend=False
    )

    track = DimTrack(
        track_id="test123",
        track_name="Test Song",
        artist_names="Test Artist",
        genre="Pop",
        duration_ms=180000,
        explicit_flag=False
    )

    session.add_all([t, track])
    session.commit()

    result = session.query(DimTrack).first()
    print(result.track_name)