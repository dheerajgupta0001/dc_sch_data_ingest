from src.config.appConfig import initConfigs
from src.config.appConfig import getFileMappings
from src.dataFetchers.dataFetcherHandler import getExcelFilePath
from src.app.gujIntradayDcService import gujIntradayDcService
from src.app.chattIntradayDcService import chattIntradayDcService
from src.app.mpIntradayDcService import mpIntradayDcService
from src.app.mhIntradayDcService import mhIntradayDcService
from src.loggerFactory import initFileLogger
import datetime as dt


initConfigs()
logger = initFileLogger("app_logger", "app_logs/app_log.log", 50, 10)

logger.info("started DC db import script")
filesSheet = getFileMappings()


endDt = dt.datetime.now()
endDt = dt.datetime(endDt.year,endDt.month,endDt.day)
targetDt =  endDt

for eachrow in filesSheet:
        print(eachrow['file_type'])
        excelFilePath = getExcelFilePath(eachrow, targetDt)
        if eachrow['file_type'] == 'guj_intraday_dc_data':
            try:
                gujIntradayDcService(excelFilePath, targetDt)
            except Exception as ex:
                logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
                print(ex)

        if eachrow['file_type'] == 'chatt_intraday_dc_data':
            try:
                chattIntradayDcService(excelFilePath, targetDt)
            except Exception as ex:
                logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
                print(ex)

        if eachrow['file_type'] == 'mp_intraday_dc_data':
            try:
                mpIntradayDcService(excelFilePath, targetDt)
            except Exception as ex:
                logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
                print(ex)

        if eachrow['file_type'] == 'mh_intraday_dc_data':
            try:
                mhIntradayDcService(excelFilePath, targetDt)
            except Exception as ex:
                logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
                print(ex)