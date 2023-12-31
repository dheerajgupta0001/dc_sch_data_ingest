from src.config.appConfig import initConfigs
from src.config.appConfig import getFileMappings
from src.dataFetchers.dataFetcherHandler import getExcelFilePath
from src.app.scheduleService.gujIntradaySchService import gujIntradaySchService
from src.app.scheduleService.chattIntradaySchService import chattIntradaySchService
from src.app.scheduleService.mpIntradaySchService import mpIntradaySchService
from src.app.scheduleService.mhIntradaySchService import mhIntradaySchService
from src.loggerFactory import initFileLogger
import datetime as dt


initConfigs()
logger = initFileLogger("app_logger", "app_logs/app_log.log", 50, 10)

logger.info("started schedule db import script")
filesSheet = getFileMappings()


endDt = dt.datetime.now()
endDt = dt.datetime(endDt.year,endDt.month,endDt.day)
targetDt =  endDt

for eachrow in filesSheet:
        print(eachrow['file_type'])
        excelFilePath = getExcelFilePath(eachrow, targetDt)
        if eachrow['file_type'] == 'guj_intraday_sch_data':
            try:
                gujIntradaySchService(excelFilePath, targetDt)
            except Exception as ex:
                logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
                print(ex)

        if eachrow['file_type'] == 'chatt_intraday_sch_data':
            try:
                chattIntradaySchService(excelFilePath, targetDt)
            except Exception as ex:
                logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
                print(ex)

        if eachrow['file_type'] == 'mp_intraday_sch_data':
            try:
                mpIntradaySchService(excelFilePath, targetDt)
            except Exception as ex:
                logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
                print(ex)

        if eachrow['file_type'] == 'mh_intraday_sch_data':
            try:
                mhIntradaySchService(excelFilePath, targetDt)
            except Exception as ex:
                logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
                print(ex)