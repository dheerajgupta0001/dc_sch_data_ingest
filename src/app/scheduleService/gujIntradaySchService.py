from src.config.appConfig import getJsonConfig
from src.scheduleDataFetcher.dataFetcherHandler import getGujIntradaySchData
from src.repos.scheduleDataRepos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.scheduleTypeRecord.gujIntradaySchRecord import IGujIntradaySchDataRecord


def gujIntradaySchService(gujIntradaySchFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'GJ'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    gujIntradaySchRecords: [IGujIntradaySchDataRecord] = getGujIntradaySchData(gujIntradaySchFilePath, unitDetailsDf, targetDt)

    isRawCreationSuccess = measDataRepo.insertGujIntradaySchData(gujIntradaySchRecords)
    if isRawCreationSuccess:
        print("Guj Intraday Sch data insertion SUCCESSFUL")
    else:
        print("Guj Intraday Sch data insertion UNSUCCESSFUL")
    return True
