from src.config.appConfig import initConfigs
from src.config.appConfig import getFileMappings
from src.dataFetchers.dataFetcherHandler import getExcelFilePath
from src.app.dayAheadDcService.gujDayAheadDcService import gujDayAheadDcService
from src.app.dayAheadDcService.chattDayAheadDcService import chattDayAheadDcService
from src.app.dayAheadDcService.mpDayAheadDcService import mpDayAheadDcService
from src.app.dayAheadDcService.mhDayAheadDcService import mhDayAheadDcService
from src.loggerFactory import initFileLogger
import datetime as dt


initConfigs()
logger = initFileLogger("app_logger", "app_logs/app_log.log", 50, 10)

logger.info("started day ahead dc data db import script")
filesSheet = getFileMappings()


endDt = dt.datetime.now()
endDt = dt.datetime(endDt.year,endDt.month,endDt.day)
targetDt =  endDt + dt.timedelta(1)

for eachrow in filesSheet:
        print(eachrow['file_type'])
        excelFilePath = getExcelFilePath(eachrow, targetDt)
        if eachrow['file_type'] == 'chatt_day_ahead_dc_data':
            try:
                chattDayAheadDcService(excelFilePath, targetDt)
            except Exception as ex:
                logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
                print(ex)

        # if eachrow['file_type'] == 'guj_day_ahead_dc_data':
        #     try:
        #         gujDayAheadDcService(excelFilePath, targetDt)
        #     except Exception as ex:
        #         logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
        #         print(ex)

        # if eachrow['file_type'] == 'mp_day_ahead_dc_data':
        #     try:
        #         mpDayAheadDcService(excelFilePath, targetDt)
        #     except Exception as ex:
        #         logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
        #         print(ex)

        # if eachrow['file_type'] == 'mh_day_ahead_dc_data':
        #     try:
        #         mhDayAheadDcService(excelFilePath, targetDt)
        #     except Exception as ex:
        #         logger.error(f"Exception occurred : {str(ex)}", exc_info=False)
        #         print(ex)