import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.chattIntradayDcRecord import IChattIntradayDcDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getChattIntradayDcData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IChattIntradayDcDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for chatt state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IChattIntradayDcDataRecord]: List of date(blockwise), unit name & DC data
    """
    chattIntradayDcRecords: List[IChattIntradayDcDataRecord] = []
    unitNamesList = unitDetailsDf['intraday_dc_file_tag'].to_list()

    # measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    chattIntradayDataDf = pd.read_csv(targetFilePath)
    chattIntradayDataDf['intraday_dc_file_tag'] = chattIntradayDataDf['Entity ID'] + " " + chattIntradayDataDf['Unit Number'].astype(str)
    chattIntradayDataDf = chattIntradayDataDf.drop(columns=['Entity ID', 'Unit Number', 'For Date'])
    chattIntradayDataDf = chattIntradayDataDf.melt(id_vars=['intraday_dc_file_tag'], value_name='dc_data', var_name= 'block_number')
    chattIntradayDcDf = pd.DataFrame(columns=['intraday_dc_file_tag', 'block_number', 'dc_data'])

    for unit in unitNamesList:
        for index, row in chattIntradayDataDf.iterrows():
            if unit == row['intraday_dc_file_tag']:
                matchingUnitList = []
                matchingUnitList.append(chattIntradayDataDf['intraday_dc_file_tag'][index])
                matchingUnitList.append(chattIntradayDataDf['block_number'][index])
                matchingUnitList.append(chattIntradayDataDf['dc_data'][index])
                chattIntradayDcDf.loc[len(chattIntradayDcDf)] = matchingUnitList

    chattIntradayDcDf = pd.pivot_table(chattIntradayDcDf, values ='dc_data', index =['block_number'],
                         columns =['intraday_dc_file_tag'])
    chattIntradayDcDf = chattIntradayDcDf.reset_index()
    dateTimeList = []
    hours = 0
    minutes = 0
    for temp in range(96):
        if temp%4 == 0 and temp >0:
            hours += 1
        dateBlock = targetDt + dt.timedelta(hours= hours, minutes= minutes)
        minutes = minutes + 15
        if minutes == 60:
            minutes = 0
        dateTimeList.append(dateBlock)

    chattIntradayDcDf['date_time'] = dateTimeList
    chattIntradayDcDf = chattIntradayDcDf.drop(columns=['block_number'])
    chattIntradayDcDf = chattIntradayDcDf.melt(id_vars=['date_time'], value_name='dc_data', var_name= 'unit_name')
    chattIntradayDcDf['dc_data'] = chattIntradayDcDf['dc_data'].round()
    chattIntradayDcDf['plant_name'] = 'xxx'
    chattIntradayDcDf['plant_id'] = 0

    for i in range(len(chattIntradayDcDf)):
        chattIntradayDcDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['intraday_dc_file_tag'] == chattIntradayDcDf.loc[i, "unit_name"]]['plant_name'].values[0]
        chattIntradayDcDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['intraday_dc_file_tag'] == chattIntradayDcDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    chattIntradayDcDf = chattIntradayDcDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    chattIntradayDcRecords = chattIntradayDcDf.to_dict('records')

    return chattIntradayDcRecords
