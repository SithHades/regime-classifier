from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, JSON
from .database import Base

class RawCandle(Base):
    __tablename__ = "raw_candles"

    time = Column(DateTime, primary_key=True)
    symbol = Column(String, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)

class ModelRegistry(Base):
    __tablename__ = "model_registry"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime)
    algorithm = Column(String)
    parameters = Column(JSON)
    is_active = Column(Boolean, default=True)
