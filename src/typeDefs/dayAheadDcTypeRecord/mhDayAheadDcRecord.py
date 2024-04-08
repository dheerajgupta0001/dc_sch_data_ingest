from typing import TypedDict
import datetime as dt


class IMhDayAheadDcDataRecord(TypedDict):
    date_time: dt.datetime
    sch_data: float
    plant_name: str
    plant_id: int