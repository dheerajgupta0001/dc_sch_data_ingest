from src.config.appConfig import getJsonConfig
from src.scheduleDataFetcher.dataFetcherHandler import getMhIntradaySchData
from src.repos.scheduleDataRepos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.scheduleTypeRecord.mhIntradaySchRecord import IMhIntradaySchDataRecord


def mhIntradaySchService(mhIntradaySchFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'MH'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    mhIntradaySchRecords: [IMhIntradaySchDataRecord] = getMhIntradaySchData(mhIntradaySchFilePath, unitDetailsDf, targetDt)

    isRawCreationSuccess = measDataRepo.insertMhIntradaySchData(mhIntradaySchRecords)
    if isRawCreationSuccess:
        print("MH Intraday Sch data insertion SUCCESSFUL")
    else:
        print("MH Intraday Sch data insertion UNSUCCESSFUL")
    return True
