from src.config.appConfig import getJsonConfig
from src.dayAheadDcDataFetcher.dataFetcherHandler import getGujDayAheadDcData
from src.repos.scheduleDataRepos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.dayAheadDcTypeRecord.gujDayAheadDcRecord import IGujDayAheadDcDataRecord


def gujDayAheadDcService(gujIntradaySchFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'GJ'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    gujDayAheadDcRecords: [IGujDayAheadDcDataRecord] = getGujDayAheadDcData(gujIntradaySchFilePath, unitDetailsDf, targetDt) # type: ignore

    isRawCreationSuccess = measDataRepo.insertGujIntradaySchData(gujDayAheadDcRecords)
    if isRawCreationSuccess:
        print("Guj Day Ahead DC data insertion SUCCESSFUL")
    else:
        print("Guj Day Ahead DC data insertion UNSUCCESSFUL")
    return True
