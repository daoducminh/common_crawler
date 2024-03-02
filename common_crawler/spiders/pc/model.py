from datetime import date

import pendulum
from sqlalchemy import TIMESTAMP, Date, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class ItemPrice(Base):
    __tablename__ = "f_price"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    price: Mapped[int] = mapped_column(Integer)
    source: Mapped[str] = mapped_column(String)
    category: Mapped[str] = mapped_column(String)
    timestamp: Mapped[pendulum.DateTime] = mapped_column(TIMESTAMP)
    ingest_date: Mapped[date] = mapped_column(Date)
