from src.config.appConfig import getJsonConfig
from src.dayAheadDcDataFetcher.dataFetcherHandler import getMhDayAheadDcData
# from src.repos.scheduleDataRepos.measDataRepo import MeasDataRepo
from src.repos.dayAheadDcDataRepos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.dayAheadDcTypeRecord.mhDayAheadDcRecord import IMhDayAheadDcDataRecord


def mhDayAheadDcService(mhIntradaySchFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'MH'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    mhDayAheadDcRecords: [IMhDayAheadDcDataRecord] = getMhDayAheadDcData(mhIntradaySchFilePath, unitDetailsDf, targetDt) # type: ignore

    isRawCreationSuccess = measDataRepo.insertMhDayAheadDcData(mhDayAheadDcRecords)
    if isRawCreationSuccess:
        print("MH Day Ahead DC data insertion SUCCESSFUL")
    else:
        print("MH Day Ahead DC data insertion UNSUCCESSFUL")
    return True
