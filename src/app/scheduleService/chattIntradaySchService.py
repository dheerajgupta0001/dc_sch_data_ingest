from src.config.appConfig import getJsonConfig
from src.scheduleDataFetcher.dataFetcherHandler import getChattIntradaySchData
from src.repos.scheduleDataRepos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.scheduleTypeRecord.chattIntradaySchRecord import IChattIntradaySchDataRecord


def chattIntradaySchService(chattIntradaySchFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'CH'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    chattIntradaySchRecords: [IChattIntradaySchDataRecord] = getChattIntradaySchData(chattIntradaySchFilePath, unitDetailsDf, targetDt)

    isRawCreationSuccess = measDataRepo.insertChattIntradaySchData(chattIntradaySchRecords)
    if isRawCreationSuccess:
        print("Chatt Intraday Sch data insertion SUCCESSFUL")
    else:
        print("Chatt Intraday Sch data insertion UNSUCCESSFUL")
    return True
