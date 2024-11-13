from src.config.appConfig import initConfigs, getJsonConfig
from src.config.appConfig import getFileMappings
from src.dataFetchers.dataFetcherHandler import getExcelFilePath
from src.app.dayAheadDcService.gujDayAheadDcService import gujDayAheadDcService
from src.app.dayAheadDcService.chattDayAheadDcService import chattDayAheadDcService
from src.app.dayAheadDcService.mpDayAheadDcService import mpDayAheadDcService
from src.app.dayAheadDcService.mhDayAheadDcService import mhDayAheadDcService
from src.loggerFactory import initFileLogger
import datetime as dt
from src.readFileFromSFTPServer.readFileFromSftp import readSftpFie


initConfigs()
jsonConfig = getJsonConfig()
sftpConfig = jsonConfig['statesInfo']
sftphost = jsonConfig['sftp_host']
logger = initFileLogger("app_logger", "app_logs/app_log.log", 50, 10)

logger.info("started day ahead dc data db import script")
filesSheet = getFileMappings()


endDt = dt.datetime.now()
endDt = dt.datetime(endDt.year,endDt.month,endDt.day)
targetDt =  endDt + dt.timedelta(1)


for sftpRow in sftpConfig:
    if sftpRow['day_ahead_file_type'] == 'chatt_day_ahead_dc_data':
        try:
            readSftpFie(sftphost, sftpRow, targetDt, False, False, True)
            excelFilePath = getExcelFilePath(jsonConfig['chatt_day_ahead_file_location'], sftpRow['filename'], sftpRow['format'], targetDt)
            chattDayAheadDcService(excelFilePath, targetDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
            print(ex)
    if sftpRow['day_ahead_file_type'] == 'guj_day_ahead_dc_data':
        try:
            dayAheadDt = endDt
            readSftpFie(sftphost, sftpRow, dayAheadDt, False, False, True)
            excelFilePath = getExcelFilePath(jsonConfig['guj_day_ahead_file_location'], sftpRow['day_ahead_filename'], sftpRow['format'], dayAheadDt)
            gujDayAheadDcService(excelFilePath, dayAheadDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
            print(ex)

    if sftpRow['day_ahead_file_type'] == 'mh_day_ahead_dc_data':
        try:
            dayAheadDt = endDt
            readSftpFie(sftphost, sftpRow, dayAheadDt, False, False, True)
            excelFilePath = getExcelFilePath(jsonConfig['mh_day_ahead_file_location'], sftpRow['day_ahead_filename'], sftpRow['format'], dayAheadDt)
            mhDayAheadDcService(excelFilePath, dayAheadDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)