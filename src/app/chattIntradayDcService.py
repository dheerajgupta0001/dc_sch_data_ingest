from src.config.appConfig import getJsonConfig
from src.dataFetchers.dataFetcherHandler import getChattIntradayDcData
from src.repos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.chattIntradayDcRecord import IChattIntradayDcDataRecord


def chattIntradayDcService(chattIntradayDcFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'CH'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    chattIntradayDcRecords: [IChattIntradayDcDataRecord] = getChattIntradayDcData(chattIntradayDcFilePath, unitDetailsDf, targetDt)

    isRawCreationSuccess = measDataRepo.insertChattIntradayDcData(chattIntradayDcRecords)
    if isRawCreationSuccess:
        print("Chatt Intraday DC data insertion SUCCESSFUL")
    else:
        print("Chatt Intraday DC data insertion UNSUCCESSFUL")
    return True
