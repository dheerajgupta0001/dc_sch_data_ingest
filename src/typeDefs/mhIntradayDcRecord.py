from typing import TypedDict
import datetime as dt


class IMhIntradayDcDataRecord(TypedDict):
    date_time: dt.datetime
    dc_data: float
    plant_name: str
    plant_id: int