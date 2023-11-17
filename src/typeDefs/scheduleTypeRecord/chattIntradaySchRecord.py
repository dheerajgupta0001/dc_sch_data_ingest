from typing import TypedDict
import datetime as dt


class IChattIntradaySchDataRecord(TypedDict):
    date_time: dt.datetime
    sch_data: float
    plant_name: str
    plant_id: int