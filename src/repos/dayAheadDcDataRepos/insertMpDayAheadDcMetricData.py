from typing_extensions import final
import psycopg2
from typing import List
import datetime as dt
from src.config.appConfig import initConfigs, getJsonConfig
from src.typeDefs.dayAheadDcTypeRecord.mpDayAheadDcRecord import IMpDayAheadDcDataRecord

initConfigs()
dbConfig = getJsonConfig()


def insertMpDayAheadDcData(dataRows: List[IMpDayAheadDcDataRecord]) -> bool:
    """Inserts a entity metrics time series data into the app db

    Args:
    appDbConnStr (str): [description]
    dataSamples (List[IMetricsDataRecord]): [description]

    Returns:
        bool: returns true if process is ok
    """
    # TODO
    return True
