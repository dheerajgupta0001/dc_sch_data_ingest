from src.config.appConfig import getJsonConfig
from src.dataFetchers.dataFetcherHandler import getMhIntradayDcData
from src.repos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.mhIntradayDcRecord import IMhIntradayDcDataRecord


def mhIntradayDcService(mhIntradayDcFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'MH'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    mhIntradayDcRecords: [IMhIntradayDcDataRecord] = getMhIntradayDcData(mhIntradayDcFilePath, unitDetailsDf, targetDt)

    isRawCreationSuccess = measDataRepo.insertMhIntradayDcData(mhIntradayDcRecords)
    if isRawCreationSuccess:
        print("MH Intraday DC data insertion SUCCESSFUL")
    else:
        print("MH Intraday DC data insertion UNSUCCESSFUL")
    return True
