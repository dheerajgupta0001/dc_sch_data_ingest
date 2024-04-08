from src.config.appConfig import getJsonConfig
from src.dayAheadDcDataFetcher.dataFetcherHandler import getChattDayAheadDcData
from src.repos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.dayAheadDcTypeRecord.chattDayAheadDcRecord import IChattDayAheadDcDataRecord


def chattDayAheadDcService(chattIntradaySchFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'CH'
    unitDetailsDf = measDataRepo.getDayAheadUnitNameForState(stateName)
    chattDayAheadDcRecords: [IChattDayAheadDcDataRecord] = getChattDayAheadDcData(chattIntradaySchFilePath, unitDetailsDf, targetDt) # type: ignore

    isRawCreationSuccess = measDataRepo.insertChattIntradayDcData(chattDayAheadDcRecords)
    if isRawCreationSuccess:
        print("Chatt Day Ahead DC data insertion SUCCESSFUL")
    else:
        print("Chatt Day Ahead DC data insertion UNSUCCESSFUL")
    return True
