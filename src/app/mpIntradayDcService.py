from src.config.appConfig import getJsonConfig
from src.dataFetchers.dataFetcherHandler import getMpIntradayDcData
from src.repos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.mpIntradayDcRecord import IMpIntradayDcDataRecord


def mpIntradayDcService(mpIntradayDcFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'MP'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    mpIntradayDcRecords: [IMpIntradayDcDataRecord] = getMpIntradayDcData(mpIntradayDcFilePath, unitDetailsDf, targetDt)

    isRawCreationSuccess = measDataRepo.insertMpIntradayDcData(mpIntradayDcRecords)
    if isRawCreationSuccess:
        print("MP Intraday DC data insertion SUCCESSFUL")
    else:
        print("MP Intraday DC data insertion UNSUCCESSFUL")
    return True
