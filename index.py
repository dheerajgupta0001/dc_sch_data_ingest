from src.config.appConfig import initConfigs
from src.config.appConfig import getFileMappings
from src.dataFetchers.dataFetcherHandler import getExcelFilePath
from src.app.gujIntradayDcService import gujIntradayDcService
from src.app.chattIntradayDcService import chattIntradayDcService
from src.app.mpIntradayDcService import mpIntradayDcService
from src.app.mhIntradayDcService import mhIntradayDcService
import datetime as dt


initConfigs()
filesSheet = getFileMappings()


endDt = dt.datetime.now()
endDt = dt.datetime(endDt.year,endDt.month,endDt.day)
targetDt =  endDt

for eachrow in filesSheet:
        print(eachrow['file_type'])
        excelFilePath = getExcelFilePath(eachrow, targetDt)
        # if eachrow['file_type'] == 'guj_intraday_dc_data':
        #     try:
        #         gujIntradayDcService(excelFilePath, targetDt)
        #     except Exception as ex:
        #         print(ex)

        # if eachrow['file_type'] == 'chatt_intraday_dc_data':
        #     try:
        #         chattIntradayDcService(excelFilePath, targetDt)
        #     except Exception as ex:
        #         print(ex)

        # if eachrow['file_type'] == 'mp_intraday_dc_data':
        #     try:
        #         mpIntradayDcService(excelFilePath, targetDt)
        #     except Exception as ex:
        #         print(ex)

        if eachrow['file_type'] == 'mh_intraday_dc_data':
            try:
                mhIntradayDcService(excelFilePath, targetDt)
            except Exception as ex:
                print(ex)