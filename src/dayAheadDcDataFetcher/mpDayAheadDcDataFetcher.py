import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.dayAheadDcTypeRecord.mpDayAheadDcRecord import IMpDayAheadDcDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getMpDayAheadDcData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IMpDayAheadDcDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for mp state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IMpDayAheadDcDataRecord]: List of date(blockwise), unit name & Sch data
    """
    # TODO MP Day Ahead Data
    mpDayAheadDcRecords: List[IMpDayAheadDcDataRecord] = []
    unitNamesList = unitDetailsDf['day_ahead_dc_file_tag'].to_list()

    mpDayAheadDataDf = pd.read_csv(targetFilePath, skiprows= 5)
    if 'Unnamed: 1' in mpDayAheadDataDf.columns:
        mpDayAheadDataDf = mpDayAheadDataDf.drop(columns=['Unnamed: 1'])
    if 'Unnamed: 98' in mpDayAheadDataDf.columns:
        mpDayAheadDataDf = mpDayAheadDataDf.drop(columns=['Unnamed: 98'])

    mpDayAheadDataDf.rename(columns = {'Block Interval':'intraday_dc_file_tag'}, inplace = True)
    mpDayAheadDataDf = mpDayAheadDataDf.melt(id_vars=['intraday_dc_file_tag'], value_name='dc_data', var_name= 'block_number')
    mpDayAheadDcDf = pd.DataFrame(columns=['intraday_dc_file_tag', 'block_number', 'dc_data'])

    # for unit in unitNamesList:
    #     for index, row in mpDayAheadDataDf.iterrows():
    #         if unit == row['intraday_dc_file_tag']:
    #             matchingUnitList = []
    #             matchingUnitList.append(mpDayAheadDataDf['intraday_dc_file_tag'][index])
    #             matchingUnitList.append(mpDayAheadDataDf['block_number'][index])
    #             matchingUnitList.append(mpDayAheadDataDf['dc_data'][index])
    #             mpDayAheadDcDf.loc[len(mpDayAheadDcDf)] = matchingUnitList

    # test starts
    mask = mpDayAheadDataDf['intraday_dc_file_tag'].isin(unitNamesList)
    # Filter DataFrame using mask and select only required columns
    result_df = mpDayAheadDataDf[mask][['intraday_dc_file_tag', 'block_number', 'dc_data']].copy()
    # Reset index to ensure continuous indexing
    result_df.reset_index(drop=True, inplace=True)
    # test ends

    mpDayAheadDcDf = pd.pivot_table(result_df, values ='dc_data', index =['block_number'],
                         columns =['intraday_dc_file_tag'])
    mpDayAheadDcDf = mpDayAheadDcDf.reset_index()
    dateTimeList = []
    hoursMinutes = mpDayAheadDcDf.iloc[:, 0]
    dateTimeList = []
    for temp in hoursMinutes:
        blockDetail = temp.split('-')[0]
        hours = int(blockDetail.split(':')[0])
        minutes = int(blockDetail.split(':')[1])
        dateBlock = targetDt + dt.timedelta(hours= hours, minutes= minutes)
        dateTimeList.append(dateBlock)


    mpDayAheadDcDf['date_time'] = dateTimeList
    mpDayAheadDcDf = mpDayAheadDcDf.drop(columns=['block_number'])
    mpDayAheadDcDf = mpDayAheadDcDf.melt(id_vars=['date_time'], value_name='dc_data', var_name= 'unit_name')
    mpDayAheadDcDf['dc_data'] = mpDayAheadDcDf['dc_data'].round()
    mpDayAheadDcDf['plant_name'] = 'xxx'
    mpDayAheadDcDf['plant_id'] = 0

    for i in range(len(mpDayAheadDcDf)):
        mpDayAheadDcDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['day_ahead_dc_file_tag'] == mpDayAheadDcDf.loc[i, "unit_name"]]['plant_name'].values[0]
        mpDayAheadDcDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['day_ahead_dc_file_tag'] == mpDayAheadDcDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    mpDayAheadDcDf = mpDayAheadDcDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    mpDayAheadDcRecords = mpDayAheadDcDf.to_dict('records')

    return mpDayAheadDcRecords