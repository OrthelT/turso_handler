import pandas as pd
from sqlalchemy import MetaData, String, Integer, DateTime, Float, Boolean, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, Mapped, mapped_column
import libsql_experimental as libsql

class Base(DeclarativeBase):
    pass

class MarketStats(Base):
    __tablename__ = "marketstats"
    type_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    total_volume_remain: Mapped[int] = mapped_column(Integer)
    min_price: Mapped[float] = mapped_column(Float)
    price: Mapped[float] = mapped_column(Float)
    avg_price: Mapped[float] = mapped_column(Float)
    avg_volume: Mapped[float] = mapped_column(Float)
    group_id: Mapped[int] = mapped_column(Integer)
    type_name: Mapped[str] = mapped_column(String)
    group_name: Mapped[str] = mapped_column(String)
    category_id: Mapped[int] = mapped_column(Integer)
    category_name: Mapped[str] = mapped_column(String)
    days_remaining: Mapped[float] = mapped_column(Float)
    last_update: Mapped[DateTime] = mapped_column(DateTime)


    def __repr__(self) -> str:
        f"""marketstats(type_id={self.type_id!r}, 
        total_volume_remain={self.total_volume_remain!r}, 
        min_price={self.min_price!r},
        price={self.price!r},
        avg_price={self.avg_price!r},
        avg_volume={self.avg_volume!r},
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
    type_name: Mapped[str] = mapped_column(String,nullable=True)
    duration: Mapped[int] = mapped_column(Integer,nullable=True)
    issued: Mapped[DateTime] = mapped_column(DateTime,nullable=True)
    price: Mapped[float] = mapped_column(Float,nullable=True)
    volume_remain: Mapped[int] = mapped_column(Integer,nullable=True)

    def __repr__(self) -> str:
        f"""marketorders(
        order_id={self.order_id!r},
        is_buy_order={self.is_buy_order!r},
        type_id={self.type_id!r},
        type_name={self.type_name!r},
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
    type_name: Mapped[str] = mapped_column(String(100))
    type_id: Mapped[str] = mapped_column(String(10))
    average: Mapped[float] = mapped_column(Float)
    volume: Mapped[int] = mapped_column(Integer)
    highest: Mapped[float] = mapped_column(Float)
    lowest: Mapped[float] = mapped_column(Float)
    order_count: Mapped[int] = mapped_column(Integer)
    timestamp: Mapped[DateTime] = mapped_column(DateTime)

class Doctrines(Base):
    __tablename__ = "doctrines"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fit_id: Mapped[int] = mapped_column(Integer)
    ship_id: Mapped[int] = mapped_column(Integer)
    ship_name: Mapped[str] = mapped_column(String)
    hulls: Mapped[int] = mapped_column(Integer)
    type_id: Mapped[int] = mapped_column(Integer)
    type_name: Mapped[str] = mapped_column(String)
    fit_qty: Mapped[int] = mapped_column(Integer)
    fits_on_mkt: Mapped[float] = mapped_column(Float)
    total_stock: Mapped[int] = mapped_column(Integer)
    price: Mapped[float] = mapped_column(Float)
    avg_vol: Mapped[float] = mapped_column(Float)
    days: Mapped[float] = mapped_column(Float)
    group_id: Mapped[int] = mapped_column(Integer)
    group_name: Mapped[str] = mapped_column(String)
    category_id: Mapped[int] = mapped_column(Integer)
    category_name: Mapped[str] = mapped_column(String)
    timestamp: Mapped[DateTime] = mapped_column(DateTime)

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
        price={self.price!r},
        avg_vol={self.avg_vol!r},
        days={self.days!r},
        group_id={self.group_id!r},
        group_name={self.group_name!r},
        category_id={self.category_id!r},
        category_name={self.category_name!r},
        timestamp={self.timestamp!r}
        )"""
class ShipTargets(Base):
    __tablename__ = "ship_targets"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fit_id: Mapped[int] = mapped_column(Integer)
    fit_name: Mapped[str] = mapped_column(String)
    ship_id: Mapped[int] = mapped_column(Integer)
    ship_name: Mapped[str] = mapped_column(String)
    ship_target: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[DateTime] = mapped_column(DateTime)

    def __repr__(self) -> str:
        f"""ship_targets(
        fit_id={self.fit_id!r},
        fit_name={self.fit_name!r},
        ship_id={self.ship_id!r},
        ship_name={self.ship_name!r},
        ship_target={self.ship_target!r},
        created_at={self.created_at!r}
        )"""

class DoctrineMap(Base):
    __tablename__ = "doctrine_map"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doctrine_id: Mapped[int] = mapped_column(Integer)
    fitting_id: Mapped[int] = mapped_column(Integer)

    def __repr__(self) -> str:
        f"""doctrine_map(
        doctrine_id={self.doctrine_id!r},
        fitting_id={self.fitting_id!r}
        )"""
class DoctrineFits(Base):
    __tablename__ = 'doctrine_fits'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    doctrine_name: Mapped[str] = mapped_column(String)
    fit_name: Mapped[str] = mapped_column(String)
    ship_type_id: Mapped[int] = mapped_column(Integer)
    doctrine_id: Mapped[int] = mapped_column(Integer)
    fit_id: Mapped[int] = mapped_column(Integer)
    ship_name: Mapped[str] = mapped_column(String)
    target: Mapped[int] = mapped_column(Integer)

    def __repr__(self):
        return f"<DoctrineFit(doctrine_name='{self.doctrine_name}', fit_name='{self.fit_name}', ship_name='{self.ship_name}')>"

class LeadShips(Base):
    __tablename__ = 'lead_ships'
    doctrine_name: Mapped[str] = mapped_column(String)
    doctrine_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    lead_ship: Mapped[int] = mapped_column(Integer)
    fit_id: Mapped[int] = mapped_column(Integer)
    
    def __repr__(self):
        return f"<LeadShips(doctrine_name='{self.doctrine_name}', doctrine_id='{self.doctrine_id}', lead_ship='{self.lead_ship}', fit_id='{self.fit_id}')>"



if __name__ == "__main__":
    pass