from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

USER = "sindri"
PASSWORD = "Ebba1505"
PORT = "5432"
DATABASE_NAME = "final_project"

# Database connection string
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@localhost:{PORT}/{DATABASE_NAME}"

# Create the SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)

# Create a configured session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Dep function that provides a DB session
def get_orkuflaedi_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
