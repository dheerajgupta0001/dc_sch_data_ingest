import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.dayAheadDcTypeRecord.mpDayAheadDcRecord import IMpDayAheadDcDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getMpDayAheadDcData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IMpDayAheadDcDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for mp state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IMpDayAheadDcDataRecord]: List of date(blockwise), unit name & Sch data
    """
    mpDayAheadDcRecords: List[IMpDayAheadDcDataRecord] = []
    unitNamesList = unitDetailsDf['intraday_sch_file_tag'].to_list()

    # TODO MP Day Ahead Data


    return mpDayAheadDcRecords
