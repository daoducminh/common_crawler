from datetime import date

import pendulum
from sqlalchemy import TIMESTAMP, BigInteger, Date, Integer, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class StgPcPrice(Base):
    __tablename__ = "stg_pc_price"

    source: Mapped[str] = mapped_column(String, primary_key=True)
    product_id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    category: Mapped[str] = mapped_column(String)
    price: Mapped[int] = mapped_column(BigInteger)
    crawled_at: Mapped[date] = mapped_column(Date)


class DpcProduct(Base):
    __tablename__ = "d_pc_product"

    product_id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str | None] = mapped_column(String)
    source: Mapped[str | None] = mapped_column(String)
    category: Mapped[str | None] = mapped_column(String)
    latest_price: Mapped[int | None] = mapped_column(BigInteger)
    latest_price_at: Mapped[pendulum.DateTime | None] = mapped_column(
        TIMESTAMP(timezone=True)
    )


class FpcPrice(Base):
    __tablename__ = "f_pc_price"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[date] = mapped_column(Date)
    product_id: Mapped[str] = mapped_column(String)
    price: Mapped[int] = mapped_column(BigInteger)
