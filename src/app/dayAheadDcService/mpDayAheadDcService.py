from src.config.appConfig import getJsonConfig
from src.dayAheadDcDataFetcher.dataFetcherHandler import getMpDayAheadDcData
from src.repos.dayAheadDcDataRepos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.dayAheadDcTypeRecord.mpDayAheadDcRecord import IMpDayAheadDcDataRecord


def mpDayAheadDcService(mpIntradaySchFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'MP'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    mpDayAheadDcRecords: [IMpDayAheadDcDataRecord] = getMpDayAheadDcData(mpIntradaySchFilePath, unitDetailsDf, targetDt) # type: ignore

    isRawCreationSuccess = measDataRepo.insertMpDayAheadDcData(mpDayAheadDcRecords)
    if isRawCreationSuccess:
        print("MP Day Ahead DC data insertion SUCCESSFUL")
    else:
        print("MP Day Ahead DC data insertion UNSUCCESSFUL")
    return True
