from src.typeDefs.fileInfo import IFileInfo
import datetime as dt
from typing import List
import os
import pandas as pd
from src.typeDefs.scheduleTypeRecord.gujIntradaySchRecord import IGujIntradaySchDataRecord
from src.typeDefs.scheduleTypeRecord.chattIntradaySchRecord import IChattIntradaySchDataRecord
from src.typeDefs.scheduleTypeRecord.mpIntradaySchRecord import IMpIntradaySchDataRecord
from src.typeDefs.scheduleTypeRecord.mhIntradaySchRecord import IMhIntradaySchDataRecord
from src.scheduleDataFetcher.chattIntradaySchDataFetcher import getChattIntradaySchData
from src.scheduleDataFetcher.mpIntradaySchDataFetcher import getMpIntradaySchData
from src.scheduleDataFetcher.gujIntradaySchDataFetcher import getGujIntradaySchData
from src.scheduleDataFetcher.mhIntradaySchDataFetcher import getMhIntradaySchData


def getExcelFilePath(fileInfo: IFileInfo, targetMonth: dt.datetime) -> str:

    targetDateStr = ''
    if not pd.isna(fileInfo['format']):
        targetDateStr = dt.datetime.strftime(targetMonth, fileInfo['format'])

    targetFilename = fileInfo['filename'].replace('{{dt}}', targetDateStr)
    targetFilePath = os.path.join(fileInfo['folder_location'], targetFilename)
    return targetFilePath


def getChattIntradaySchDataHandler(targetFilePath: str) -> List[IChattIntradaySchDataRecord]:
    return getChattIntradaySchData(targetFilePath)

def getMpIntradaySchDataHandler(targetFilePath: str) -> List[IMpIntradaySchDataRecord]:
    return getMpIntradaySchData(targetFilePath)

def getGujIntradaySchDataHandler(targetFilePath: str) -> List[IGujIntradaySchDataRecord]:
    return getGujIntradaySchData(targetFilePath)

def getMhIntradaySchDataHandler(targetFilePath: str) -> List[IMhIntradaySchDataRecord]:
    return getMhIntradaySchData(targetFilePath)
