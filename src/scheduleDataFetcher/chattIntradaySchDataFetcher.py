import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.scheduleTypeRecord.chattIntradaySchRecord import IChattIntradaySchDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getChattIntradaySchData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IChattIntradaySchDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for chatt state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IChattIntradaySchDataRecord]: List of date(blockwise), unit name & Sch data
    """
    chattIntradaySchRecords: List[IChattIntradaySchDataRecord] = []
    unitNamesList = unitDetailsDf['intraday_sch_file_tag'].to_list()

    chattIntradayDataDf = pd.read_csv(targetFilePath)
    chattIntradayDataDf['intraday_sch_file_tag'] = chattIntradayDataDf['Entity ID']
    chattIntradayDataDf = chattIntradayDataDf.drop(columns=['Entity ID', 'For Date'])
    chattIntradayDataDf = chattIntradayDataDf.melt(id_vars=['intraday_sch_file_tag'], value_name='sch_data', var_name= 'block_number')
    chattIntradaySchDf = pd.DataFrame(columns=['intraday_sch_file_tag', 'block_number', 'sch_data'])

    for unit in unitNamesList:
        for index, row in chattIntradayDataDf.iterrows():
            if unit == row['intraday_sch_file_tag']:
                matchingUnitList = []
                matchingUnitList.append(chattIntradayDataDf['intraday_sch_file_tag'][index])
                matchingUnitList.append(chattIntradayDataDf['block_number'][index])
                matchingUnitList.append(chattIntradayDataDf['sch_data'][index])
                chattIntradaySchDf.loc[len(chattIntradaySchDf)] = matchingUnitList

    chattIntradaySchDf = pd.pivot_table(chattIntradaySchDf, values ='sch_data', index =['block_number'],
                         columns =['intraday_sch_file_tag'])
    chattIntradaySchDf = chattIntradaySchDf.reset_index()
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

    chattIntradaySchDf['date_time'] = dateTimeList
    chattIntradaySchDf = chattIntradaySchDf.drop(columns=['block_number'])
    chattIntradaySchDf = chattIntradaySchDf.melt(id_vars=['date_time'], value_name='sch_data', var_name= 'unit_name')
    chattIntradaySchDf['sch_data'] = chattIntradaySchDf['sch_data'].round()
    chattIntradaySchDf['plant_name'] = 'xxx'
    chattIntradaySchDf['plant_id'] = 0

    for i in range(len(chattIntradaySchDf)):
        chattIntradaySchDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['intraday_sch_file_tag'] == chattIntradaySchDf.loc[i, "unit_name"]]['plant_name'].values[0]
        chattIntradaySchDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['intraday_sch_file_tag'] == chattIntradaySchDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    chattIntradaySchDf = chattIntradaySchDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    chattIntradaySchRecords = chattIntradaySchDf.to_dict('records')

    return chattIntradaySchRecords
