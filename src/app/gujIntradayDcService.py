from src.config.appConfig import getJsonConfig
from src.dataFetchers.dataFetcherHandler import getGujIntradayDcData
from src.repos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.gujIntradayDcRecord import IGujIntradayDcDataRecord


def gujIntradayDcService(gujIntradayDcFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'GJ'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    gujIntradayDcRecords: [IGujIntradayDcDataRecord] = getGujIntradayDcData(gujIntradayDcFilePath, unitDetailsDf, targetDt)

    isRawCreationSuccess = measDataRepo.insertGujIntradayDcData(gujIntradayDcRecords)
    if isRawCreationSuccess:
        print("Guj Intraday DC data insertion SUCCESSFUL")
    else:
        print("Guj Intraday DC data insertion UNSUCCESSFUL")
    return True
