from src.config.appConfig import initConfigs, getJsonConfig
from src.config.appConfig import getFileMappings
from src.dataFetchers.dataFetcherHandler import getExcelFilePath
from src.app.gujIntradayDcService import gujIntradayDcService
from src.app.chattIntradayDcService import chattIntradayDcService
from src.app.mpIntradayDcService import mpIntradayDcService
from src.app.mhIntradayDcService import mhIntradayDcService
from src.loggerFactory import initFileLogger
import datetime as dt
from src.readFileFromSFTPServer.readFileFromSftp import readSftpFie


initConfigs()
jsonConfig = getJsonConfig()
sftpConfig = jsonConfig['statesInfo']
sftphost = jsonConfig['sftp_host']
logger = initFileLogger("app_logger", "app_logs/app_log.log", 50, 10)

logger.info("started DC db import script")
filesSheet = getFileMappings()

endDt = dt.datetime.now()
endDt = dt.datetime(endDt.year,endDt.month,endDt.day)
targetDt =  endDt

for sftpRow in sftpConfig:
    if sftpRow['dc_file_type'] == 'guj_intraday_dc_data':
        try:
            readSftpFie(sftphost, sftpRow, targetDt, True, False, False)
            excelFilePath = getExcelFilePath(jsonConfig['guj_file_location'], sftpRow['filename'], sftpRow['format'], targetDt)
            gujIntradayDcService(excelFilePath, targetDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
            print(ex)

    if sftpRow['dc_file_type'] == 'chatt_intraday_dc_data':
        try:
            readSftpFie(sftphost, sftpRow, targetDt, True, False, False)
            excelFilePath = getExcelFilePath(jsonConfig['chatt_file_location'], sftpRow['filename'], sftpRow['format'], targetDt)
            chattIntradayDcService(excelFilePath, targetDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
            print(ex)

    if sftpRow['dc_file_type'] == 'mp_intraday_dc_data':
        try:
            readSftpFie(sftphost, sftpRow, targetDt, True, False, False)
            excelFilePath = getExcelFilePath(jsonConfig['mp_file_location'], sftpRow['filename'], sftpRow['format'], targetDt)
            mpIntradayDcService(excelFilePath, targetDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
            print(ex)

    if sftpRow['dc_file_type'] == 'mh_intraday_dc_data':
        try:
            readSftpFie(sftphost, sftpRow, targetDt, True, False, False)
            excelFilePath = getExcelFilePath(jsonConfig['mah_file_location'], sftpRow['filename'], sftpRow['format'], targetDt)
            mhIntradayDcService(excelFilePath, targetDt)
        except Exception as ex:
            logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
            print(ex)