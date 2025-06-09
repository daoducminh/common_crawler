from datetime import date

import pendulum
from sqlalchemy import TIMESTAMP, Date, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from common_crawler.spiders.cellphones.constants import DEFAULT_TZ


class Base(DeclarativeBase):
    pass


class ItemPrice(Base):
    __tablename__ = "f_phone_price"

    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    price: Mapped[int] = mapped_column(Integer)
    category_id: Mapped[int] = mapped_column(Integer)
    chipset: Mapped[str | None] = mapped_column(String, nullable=True)
    memory: Mapped[str | None] = mapped_column(String, nullable=True)
    battery: Mapped[str | None] = mapped_column(String, nullable=True)
    display_resolution: Mapped[str | None] = mapped_column(String, nullable=True)
    display_size: Mapped[str | None] = mapped_column(String, nullable=True)
    display_type: Mapped[str | None] = mapped_column(String, nullable=True)
    nfc: Mapped[bool | None] = mapped_column(Integer, nullable=True)
    storage: Mapped[str | None] = mapped_column(String, nullable=True)
    camera_primary: Mapped[str | None] = mapped_column(String, nullable=True)
    camera_secondary: Mapped[str | None] = mapped_column(String, nullable=True)
    camera_video: Mapped[str | None] = mapped_column(String, nullable=True)
    ingest_time: Mapped[pendulum.DateTime] = mapped_column(TIMESTAMP(DEFAULT_TZ))
    ingest_date: Mapped[date] = mapped_column(Date)
