from src.typeDefs.fileInfo import IFileInfo
import datetime as dt
from typing import List
import os
import pandas as pd
from src.typeDefs.gujIntradayDcRecord import IGujIntradayDcDataRecord
from src.typeDefs.chattIntradayDcRecord import IChattIntradayDcDataRecord
from src.typeDefs.mpIntradayDcRecord import IMpIntradayDcDataRecord
from src.typeDefs.mhIntradayDcRecord import IMhIntradayDcDataRecord
from src.dataFetchers.gujIntradayDcDataFetcher import getGujIntradayDcData
from src.dataFetchers.chattIntradayDcDataFetcher import getChattIntradayDcData
from src.dataFetchers.mpIntradayDcDataFetcher import getMpIntradayDcData
from src.dataFetchers.mhIntradayDcDataFetcher import getMhIntradayDcData


def getExcelFilePath(fileInfo: IFileInfo, targetMonth: dt.datetime) -> str:

    targetDateStr = ''
    if not pd.isna(fileInfo['format']):
        targetDateStr = dt.datetime.strftime(targetMonth, fileInfo['format'])

    targetFilename = fileInfo['filename'].replace('{{dt}}', targetDateStr)
    targetFilePath = os.path.join(fileInfo['folder_location'], targetFilename)
    return targetFilePath


def getGujIntradayDcDataHandler(targetFilePath: str) -> List[IGujIntradayDcDataRecord]:
    return getGujIntradayDcData(targetFilePath)

def getChattIntradayDcDataHandler(targetFilePath: str) -> List[IChattIntradayDcDataRecord]:
    return getChattIntradayDcData(targetFilePath)

def getMpIntradayDcDataHandler(targetFilePath: str) -> List[IMpIntradayDcDataRecord]:
    return getMpIntradayDcData(targetFilePath)

def getMhIntradayDcDataHandler(targetFilePath: str) -> List[IMhIntradayDcDataRecord]:
    return getMhIntradayDcData(targetFilePath)
