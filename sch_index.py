from src.config.appConfig import initConfigs, getJsonConfig
from src.config.appConfig import getFileMappings
from src.dataFetchers.dataFetcherHandler import getExcelFilePath
from src.app.scheduleService.gujIntradaySchService import gujIntradaySchService
from src.app.scheduleService.chattIntradaySchService import chattIntradaySchService
from src.app.scheduleService.mpIntradaySchService import mpIntradaySchService
from src.app.scheduleService.mhIntradaySchService import mhIntradaySchService
from src.loggerFactory import initFileLogger
import datetime as dt
from src.readFileFromSFTPServer.readFileFromSftp import readSftpFie


initConfigs()
jsonConfig = getJsonConfig()
sftpConfig = jsonConfig['statesInfo']
sftphost = jsonConfig['sftp_host']
logger = initFileLogger("app_logger", "app_logs/app_log.log", 50, 10)

logger.info("started schedule db import script")
filesSheet = getFileMappings()

endDt = dt.datetime.now()
endDt = dt.datetime(endDt.year,endDt.month,endDt.day)
targetDt =  endDt


for sftpRow in sftpConfig:
    if sftpRow['sch_file_type'] == 'guj_intraday_sch_data':
        try:
            readSftpFie(sftphost, sftpRow, targetDt, False, True, False)
            excelFilePath = getExcelFilePath(jsonConfig['guj_file_location'], sftpRow['sch_filename'], sftpRow['format'], targetDt)
            gujIntradaySchService(excelFilePath, targetDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
            print(ex)

    if sftpRow['sch_file_type'] == 'chatt_intraday_sch_data':
        try:
            readSftpFie(sftphost, sftpRow, targetDt, False, True, False)
            excelFilePath = getExcelFilePath(jsonConfig['chatt_file_location'], sftpRow['sch_filename'], sftpRow['format'], targetDt)
            chattIntradaySchService(excelFilePath, targetDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
            print(ex)

    if sftpRow['sch_file_type'] == 'mp_intraday_sch_data':
        try:
            readSftpFie(sftphost, sftpRow, targetDt, False, True, False)
            excelFilePath = getExcelFilePath(jsonConfig['mp_file_location'], sftpRow['sch_filename'], sftpRow['format'], targetDt)
            mpIntradaySchService(excelFilePath, targetDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
            print(ex)

    if sftpRow['sch_file_type'] == 'mh_intraday_sch_data':
        try:
            readSftpFie(sftphost, sftpRow, targetDt, False, True, False)
            excelFilePath = getExcelFilePath(jsonConfig['mah_file_location'], sftpRow['sch_filename'], sftpRow['format'], targetDt)
            mhIntradaySchService(excelFilePath, targetDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
            print(ex)