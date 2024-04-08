import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.dayAheadDcTypeRecord.chattDayAheadDcRecord import IChattDayAheadDcDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getChattDayAheadDcData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IChattDayAheadDcDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for chatt state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IChattDayAheadDcDataRecord]: List of date(blockwise), unit name & Sch data
    """
    chattDayAheadDcRecords: List[IChattDayAheadDcDataRecord] = []
    unitNamesList = unitDetailsDf['day_ahead_dc_file_tag'].to_list()

    chattDayAheadDcDataDf = pd.read_csv(targetFilePath)
    chattDayAheadDcDataDf['day_ahead_dc_file_tag'] = chattDayAheadDcDataDf['Entity ID'] + " " + chattDayAheadDcDataDf['Unit Number'].astype(str)
    chattDayAheadDcDataDf = chattDayAheadDcDataDf.drop(columns=['Entity ID', 'Unit Number', 'For Date'])
    chattDayAheadDcDataDf = chattDayAheadDcDataDf.melt(id_vars=['day_ahead_dc_file_tag'], value_name='dc_data', var_name= 'block_number')
    chattDayAheadDcDf = pd.DataFrame(columns=['day_ahead_dc_file_tag', 'block_number', 'dc_data'])

    for unit in unitNamesList:
        for index, row in chattDayAheadDcDataDf.iterrows():
            if unit == row['day_ahead_dc_file_tag']:
                matchingUnitList = []
                matchingUnitList.append(chattDayAheadDcDataDf['day_ahead_dc_file_tag'][index])
                matchingUnitList.append(chattDayAheadDcDataDf['block_number'][index])
                matchingUnitList.append(chattDayAheadDcDataDf['dc_data'][index])
                chattDayAheadDcDf.loc[len(chattDayAheadDcDf)] = matchingUnitList

    chattDayAheadDcDf = pd.pivot_table(chattDayAheadDcDf, values ='dc_data', index =['block_number'],
                         columns =['day_ahead_dc_file_tag'])
    chattDayAheadDcDf = chattDayAheadDcDf.reset_index()
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

    chattDayAheadDcDf['date_time'] = dateTimeList
    chattDayAheadDcDf = chattDayAheadDcDf.drop(columns=['block_number'])
    chattDayAheadDcDf = chattDayAheadDcDf.melt(id_vars=['date_time'], value_name='dc_data', var_name= 'unit_name')
    chattDayAheadDcDf['dc_data'] = chattDayAheadDcDf['dc_data'].round()
    chattDayAheadDcDf['plant_name'] = 'xxx'
    chattDayAheadDcDf['plant_id'] = 0

    for i in range(len(chattDayAheadDcDf)):
        chattDayAheadDcDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['day_ahead_dc_file_tag'] == chattDayAheadDcDf.loc[i, "unit_name"]]['plant_name'].values[0]
        chattDayAheadDcDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['day_ahead_dc_file_tag'] == chattDayAheadDcDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    chattDayAheadDcDf = chattDayAheadDcDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    chattDayAheadDcRecords = chattDayAheadDcDf.to_dict('records')

    return chattDayAheadDcRecords
