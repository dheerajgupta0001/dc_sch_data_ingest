from src.config.appConfig import getJsonConfig
from src.scheduleDataFetcher.dataFetcherHandler import getMpIntradaySchData
from src.repos.scheduleDataRepos.measDataRepo import MeasDataRepo
from typing import List
import datetime as dt
from src.typeDefs.scheduleTypeRecord.mpIntradaySchRecord import IMpIntradaySchDataRecord


def mpIntradaySchService(mpIntradaySchFilePath: str, targetDt: dt.datetime):
    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    stateName = 'MP'
    unitDetailsDf = measDataRepo.getUnitNameForState(stateName)
    mpIntradaySchRecords: [IMpIntradaySchDataRecord] = getMpIntradaySchData(mpIntradaySchFilePath, unitDetailsDf, targetDt)

    isRawCreationSuccess = measDataRepo.insertMpIntradaySchData(mpIntradaySchRecords)
    if isRawCreationSuccess:
        print("MP Intraday Sch data insertion SUCCESSFUL")
    else:
        print("MP Intraday Sch data insertion UNSUCCESSFUL")
    return True
