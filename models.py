import pandas as pd
from sqlalchemy import String, Integer, DateTime, Float, Boolean, create_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column

mkt_db = 'wcmkt.db'

class Base(DeclarativeBase):
    pass

class MarketStats(Base):
    __tablename__ = "marketstats"
    type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    total_volume_remain: Mapped[int] = mapped_column(Integer, nullable=True)
    min_price: Mapped[float] = mapped_column(Float, nullable=True)
    price: Mapped[float] = mapped_column(Float, nullable=True)
    avg_price: Mapped[float] = mapped_column(Float, nullable=True)
    avg_volume: Mapped[float] = mapped_column(Float, nullable=True)
    group_id: Mapped[int] = mapped_column(Integer, nullable=True)
    type_name: Mapped[str] = mapped_column(String, nullable=True)
    group_name: Mapped[str] = mapped_column(String, nullable=True)
    category_id: Mapped[int] = mapped_column(Integer, nullable=True)
    category_name: Mapped[str] = mapped_column(String, nullable=True)
    days_remaining: Mapped[float] = mapped_column(Float, nullable=True)
    last_update: Mapped[DateTime] = mapped_column(DateTime, nullable=True)


    def __repr__(self) -> str:
        f"""marketstats(type_id={self.type_id!r}, 
        total_volume_remain={self.total_volume_remain!r}, 
        min_price={self.min_price!r},
        price_5th_percentile={self.price_5th_percentile!r},
        avg_of_avg_price={self.avg_of_avg_price!r},
        avg_daily_volume={self.avg_daily_volume!r},
        group_id={self.group_id!r},
        type_name={self.type_name!r},   
        group_name={self.group_name!r},
        category_id={self.category_id!r},
        category_name={self.category_name!r},
        days_remaining={self.days_remaining!r},
        last_update={self.last_update!r}
        """

class MarketOrders(Base):
    __tablename__ = "marketorders"
    order_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    is_buy_order: Mapped[bool] = mapped_column(Boolean,nullable=True)
    type_id: Mapped[int] = mapped_column(Integer,nullable=True)
    duration: Mapped[int] = mapped_column(Integer,nullable=True)
    issued: Mapped[DateTime] = mapped_column(DateTime,nullable=True)
    price: Mapped[float] = mapped_column(Float,nullable=True)
    volume_remain: Mapped[int] = mapped_column(Integer,nullable=True)

    def __repr__(self) -> str:
        f"""marketorders(
        order_id={self.order_id!r},
        is_buy_order={self.is_buy_order!r},
        type_id={self.type_id!r},
        duration={self.duration!r},
        issued={self.issued!r},
        location_id={self.location_id!r},
        min_volume={self.min_volume!r},
        price={self.price!r},
        volume_remain={self.volume_remain!r},
        volume_total={self.volume_total!r},
        range={self.range!r},
        order_type={self.order_type!r}
        )"""

class MarketHistory(Base):
    __tablename__ = "market_history"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    date: Mapped[DateTime] = mapped_column(DateTime)
    type_name: Mapped[str] = mapped_column(String(100), nullable=True)
    type_id: Mapped[str] = mapped_column(String(10))
    average: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)
    highest: Mapped[float] = mapped_column(Float)
    lowest: Mapped[float] = mapped_column(Float)
    order_count: Mapped[int] = mapped_column(Integer)
    last_update: Mapped[DateTime] = mapped_column(DateTime, nullable=True)

class Doctrines(Base):
    __tablename__ = "doctrines"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fit_id: Mapped[int] = mapped_column(Integer)
    ship_id: Mapped[int] = mapped_column(Integer, nullable=True)
    ship_name: Mapped[str] = mapped_column(String, nullable=True)
    hulls: Mapped[int] = mapped_column(Integer, nullable=True)
    type_id: Mapped[int] = mapped_column(Integer, nullable=True)
    type_name: Mapped[str] = mapped_column(String, nullable=True)
    fit_qty: Mapped[int] = mapped_column(Integer, nullable=True)
    fits_on_mkt: Mapped[float] = mapped_column(Float, nullable=True)
    total_stock: Mapped[int] = mapped_column(Integer, nullable=True)
    four_hour_price: Mapped[float] = mapped_column(Float, nullable=True)
    avg_vol: Mapped[float] = mapped_column(Float, nullable=True)
    days: Mapped[float] = mapped_column(Float, nullable=True)
    group_id: Mapped[int] = mapped_column(Integer, nullable=True)
    group_name: Mapped[str] = mapped_column(String, nullable=True)
    category_id: Mapped[int] = mapped_column(Integer, nullable=True)
    category_name: Mapped[str] = mapped_column(String, nullable=True)
    timestamp: Mapped[DateTime] = mapped_column(DateTime, nullable=True)

    def __repr__(self) -> str:
        f"""doctrines(
        fit_id={self.fit_id!r},
        ship_id={self.ship_id!r},
        ship_name={self.ship_name!r},
        hulls={self.hulls!r},
        type_id={self.type_id!r},
        type_name={self.type_name!r},
        fit_qty={self.fit_qty!r},
        fits_on_mkt={self.fits_on_mkt!r},
        total_stock={self.total_stock!r},
        four_hour_price={self.four_hour_price!r},
        avg_vol={self.avg_vol!r},
        days={self.days!r},
        group_id={self.group_id!r},
        group_name={self.group_name!r},
        category_id={self.category_id!r},
        category_name={self.category_name!r},
        timestamp={self.timestamp!r}
        )"""

if __name__ == "__main__":
    pass