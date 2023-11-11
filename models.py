from sqlalchemy import Integer, String, Float, Boolean
from sqlalchemy.sql.schema import Column
from database import Base


class WorkerAn(Base):
    __tablename__ = 'worker_analitika'
    fio = Column(String, primary_key=True)

    quantity_tasks = Column(Integer)
    total_time_way = Column(Integer)
    total_distance = Column(Float)
    total_time_tasks = Column(Float)
    mean_time_way = Column(Float)


class WorkersTask(Base):
    __tablename__ = 'workers_task'
    id = Column(Integer, primary_key=True)

    fio = Column(String)
    graid = Column(String)
    address = Column(String)
    current_address = Column(String)
    busy_until = Column(String)


class TimesheetTask(Base):
    __tablename__ = 'timesheet_task'
    id = Column(Integer, primary_key=True)
    fio = Column(String)
    name = Column(String)
    address = Column(String)
    point = Column(String)
    coordinates_start = Column(String)
    coordinates_finish = Column(String)
    route_time = Column(String)
    distance = Column(String)
    time_start = Column(String)
    time_finish = Column(String)
    priority = Column(String)
    status = Column(Integer)


class Point(Base):
    __tablename__ = 'day_tasks'
    id = Column(Integer, primary_key=True)
    address = Column(String)
    date_connected = Column(String)
    all_received = Column(Boolean)
    days_from_last_card = Column(Integer)
    approved_cards = Column(Integer)
    priority = Column(Integer)
    quantity_cards = Column(Integer)
    coordinares = Column(String)



class Worker(Base):
    __tablename__ = 'workers'
    id = Column(Integer, primary_key=True)
    fio = Column(String)
    password = Column(String)



