from src.typeDefs.fileInfo import IFileInfo
import datetime as dt
from typing import List
import os
import pandas as pd
from src.typeDefs.dayAheadDcTypeRecord.gujDayAheadDcRecord import IGujDayAheadDcDataRecord
from src.typeDefs.dayAheadDcTypeRecord.chattDayAheadDcRecord import IChattDayAheadDcDataRecord
from src.typeDefs.dayAheadDcTypeRecord.mpDayAheadDcRecord import IMpDayAheadDcDataRecord
from src.typeDefs.dayAheadDcTypeRecord.mhDayAheadDcRecord import IMhDayAheadDcDataRecord
from src.dayAheadDcDataFetcher.chattDayAheadDcDataFetcher import getChattDayAheadDcData
from src.dayAheadDcDataFetcher.mpDayAheadDcDataFetcher import getMpDayAheadDcData
from src.dayAheadDcDataFetcher.gujDayAheadDcDataFetcher import getGujDayAheadDcData
from src.dayAheadDcDataFetcher.mhDayAheadDcDataFetcher import getMhDayAheadDcData


def getExcelFilePath(fileInfo: IFileInfo, targetMonth: dt.datetime) -> str:

    targetDateStr = ''
    if not pd.isna(fileInfo['format']):
        targetDateStr = dt.datetime.strftime(targetMonth, fileInfo['format'])

    targetFilename = fileInfo['filename'].replace('{{dt}}', targetDateStr)
    targetFilePath = os.path.join(fileInfo['folder_location'], targetFilename)
    return targetFilePath


def getChattDayAheadDcDataHandler(targetFilePath: str) -> List[IChattDayAheadDcDataRecord]:
    return getChattDayAheadDcData(targetFilePath)

def getMpDayAheadDcDataHandler(targetFilePath: str) -> List[IMpDayAheadDcDataRecord]:
    return getMpDayAheadDcData(targetFilePath)

def getGujDayAheadDcDataHandler(targetFilePath: str) -> List[IGujDayAheadDcDataRecord]:
    return getGujDayAheadDcData(targetFilePath)

def getMhDayAheadDcDataHandler(targetFilePath: str) -> List[IMhDayAheadDcDataRecord]:
    return getMhDayAheadDcData(targetFilePath)
