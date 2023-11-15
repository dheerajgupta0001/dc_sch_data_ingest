import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.mpIntradayDcRecord import IMpIntradayDcDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getMpIntradayDcData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IMpIntradayDcDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for mp state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IMpIntradayDcDataRecord]: List of date(blockwise), unit name & DC data
    """
    mpIntradayDcRecords: List[IMpIntradayDcDataRecord] = []
    unitNamesList = unitDetailsDf['intraday_dc_file_tag'].to_list()

    # measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    mpIntradayDataDf = pd.read_csv(targetFilePath, skiprows= 5)
    if 'Unnamed: 1' in mpIntradayDataDf.columns:
        mpIntradayDataDf = mpIntradayDataDf.drop(columns=['Unnamed: 1'])
    if 'Unnamed: 98' in mpIntradayDataDf.columns:
        mpIntradayDataDf = mpIntradayDataDf.drop(columns=['Unnamed: 98'])

    mpIntradayDataDf.rename(columns = {'Block Interval':'intraday_dc_file_tag'}, inplace = True)
    mpIntradayDataDf = mpIntradayDataDf.melt(id_vars=['intraday_dc_file_tag'], value_name='dc_data', var_name= 'block_number')
    mpIntradayDcDf = pd.DataFrame(columns=['intraday_dc_file_tag', 'block_number', 'dc_data'])

    for unit in unitNamesList:
        for index, row in mpIntradayDataDf.iterrows():
            if unit == row['intraday_dc_file_tag']:
                matchingUnitList = []
                matchingUnitList.append(mpIntradayDataDf['intraday_dc_file_tag'][index])
                matchingUnitList.append(mpIntradayDataDf['block_number'][index])
                matchingUnitList.append(mpIntradayDataDf['dc_data'][index])
                mpIntradayDcDf.loc[len(mpIntradayDcDf)] = matchingUnitList

    mpIntradayDcDf = pd.pivot_table(mpIntradayDcDf, values ='dc_data', index =['block_number'],
                         columns =['intraday_dc_file_tag'])
    mpIntradayDcDf = mpIntradayDcDf.reset_index()
    dateTimeList = []
    hoursMinutes = mpIntradayDcDf.iloc[:, 0]
    dateTimeList = []
    for temp in hoursMinutes:
        blockDetail = temp.split('-')[0]
        hours = int(blockDetail.split(':')[0])
        minutes = int(blockDetail.split(':')[1])
        dateBlock = targetDt + dt.timedelta(hours= hours, minutes= minutes)
        dateTimeList.append(dateBlock)


    mpIntradayDcDf['date_time'] = dateTimeList
    mpIntradayDcDf = mpIntradayDcDf.drop(columns=['block_number'])
    mpIntradayDcDf = mpIntradayDcDf.melt(id_vars=['date_time'], value_name='dc_data', var_name= 'unit_name')
    mpIntradayDcDf['dc_data'] = mpIntradayDcDf['dc_data'].round()
    mpIntradayDcDf['plant_name'] = 'xxx'
    mpIntradayDcDf['plant_id'] = 0

    for i in range(len(mpIntradayDcDf)):
        mpIntradayDcDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['intraday_dc_file_tag'] == mpIntradayDcDf.loc[i, "unit_name"]]['plant_name'].values[0]
        mpIntradayDcDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['intraday_dc_file_tag'] == mpIntradayDcDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    mpIntradayDcDf = mpIntradayDcDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    mpIntradayDcRecords = mpIntradayDcDf.to_dict('records')

    return mpIntradayDcRecords
