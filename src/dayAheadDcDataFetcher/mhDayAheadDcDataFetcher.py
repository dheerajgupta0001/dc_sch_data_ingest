import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.dayAheadDcTypeRecord.mhDayAheadDcRecord import IMhDayAheadDcDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getMhDayAheadDcData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IMhDayAheadDcDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for mh state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IMhDayAheadDcDataRecord]: List of date(blockwise), unit name & Sch data
    """
    mhDayAheadDcRecords: List[IMhDayAheadDcDataRecord] = []
    unitNamesList = unitDetailsDf['day_ahead_dc_file_tag'].to_list()

    # check how many entities are clubbed using comma separated in master table
    commaSeparatedList = []
    for i in range(len(unitNamesList)):
        if (unitNamesList[i].count("$") + 1)>1:
            commaSeparatedList.append(unitNamesList[i])

    # mhIntradayDataDf = pd.read_csv(targetFilePath, skiprows=5, usecols=range(98))
    mhDayAheadDataDf = pd.read_excel(targetFilePath, skiprows=5, nrows = 97)
    # mhIntradayDataDf = mhIntradayDataDf.loc[:, ~mhIntradayDataDf.columns.str.contains('^Unnamed')]
    dateTimeList = []
    hours = 0
    minutes = 0
    targetDt =  targetDt + dt.timedelta(1)
    for temp in range(96):
        if temp%4 == 0 and temp >0:
            hours += 1
        dateBlock = targetDt + dt.timedelta(hours= hours, minutes= minutes)
        minutes = minutes + 15
        if minutes == 60:
            minutes = 0
        dateTimeList.append(dateBlock)
    mhDayAheadDataDf = mhDayAheadDataDf.loc[:, ~mhDayAheadDataDf.columns.str.contains('^Unnamed')]
    mhDayAheadDataDf = mhDayAheadDataDf.iloc[1:]
    mhDayAheadDataDf= mhDayAheadDataDf.drop('TOTAL', axis=1)

    # section for handling dollar separated unit starts
    for temp in commaSeparatedList:
        tempList = temp.split('$')
        matchingList = []
        for unit in tempList:
            if unit in mhDayAheadDataDf.columns:
                matchingList.append(unit)
        combinedGasData = mhDayAheadDataDf[matchingList]
        mhDayAheadDataDf[temp] = combinedGasData.sum(axis=1)

    # section for handling matching columns starts
    matchingUnitNamesList = []
    for unit in unitNamesList:
        if unit in mhDayAheadDataDf.columns:
            matchingUnitNamesList.append(unit)
    mhDayAheadDcDf =  mhDayAheadDataDf[matchingUnitNamesList]
    # check matching columns ends

    mhDayAheadDcDf['date_time'] = dateTimeList
    mhDayAheadDcDf = mhDayAheadDcDf.melt(id_vars=['date_time'], value_name='dc_data', var_name= 'unit_name')
    mhDayAheadDcDf['dc_data'] = mhDayAheadDcDf['dc_data'].astype(int)
    mhDayAheadDcDf['plant_name'] = 'xxx'
    mhDayAheadDcDf['plant_id'] = 0

    for i in range(len(mhDayAheadDcDf)):
        mhDayAheadDcDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['day_ahead_dc_file_tag'] == mhDayAheadDcDf.loc[i, "unit_name"]]['plant_name'].values[0]
        mhDayAheadDcDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['day_ahead_dc_file_tag'] == mhDayAheadDcDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    mhDayAheadDcDf = mhDayAheadDcDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    mhDayAheadDcRecords = mhDayAheadDcDf.to_dict('records')

    return mhDayAheadDcRecords