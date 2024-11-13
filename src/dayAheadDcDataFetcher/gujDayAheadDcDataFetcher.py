import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.dayAheadDcTypeRecord.gujDayAheadDcRecord import IGujDayAheadDcDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getGujDayAheadDcData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IGujDayAheadDcDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for guj state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IGujDayAheadDcDataRecord]: List of date(blockwise), unit name & DC data
    """
    gujDayAheadDcRecords: List[IGujDayAheadDcDataRecord] = []
    unitNamesList = unitDetailsDf['day_ahead_dc_file_tag'].to_list()

    # separate the comma separated flags
    tempListWithCommaSepValues = [map(lambda x: x.strip(), item.split('$')) for item in unitNamesList]
    unitNamesListWithCommaSep = [item for sub_list in tempListWithCommaSepValues for item in sub_list]

    # check how many entities are clubbed using comma separated in master table
    commaSeparatedList = []
    for i in range(len(unitNamesList)):
        if (unitNamesList[i].count("$") + 1)>1:
            commaSeparatedList.append(unitNamesList[i])
    # targetFilePath = r'\\\\10.2.100.239\\Guj_Data\\Guj_Intraday_DC_Sch_Files\\Declared_Capacity_27-09-2024.csv'
    gujDayAheadDataDf = pd.read_csv(
        targetFilePath, nrows= 96)
    
    # check matching columns starts
    for temp in commaSeparatedList:
        tempList = temp.split('$')
        matchingList = []
        for unit in tempList:
            if unit in gujDayAheadDataDf.columns:
                matchingList.append(unit)
        combinedGasData = gujDayAheadDataDf[matchingList]
        gujDayAheadDataDf[temp] = combinedGasData.sum(axis=1)
    # check matching columns ends

    # measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    matchingUnitNamesList = []
    for unit in unitNamesList:
        if unit in gujDayAheadDataDf.columns:
            matchingUnitNamesList.append(unit)
    gujDayAheadDcDf =  gujDayAheadDataDf[matchingUnitNamesList]
    hoursMinutes = gujDayAheadDataDf.iloc[:, 0]
    dateTimeList = []
    targetDt =  targetDt + dt.timedelta(1)
    for temp in hoursMinutes:
        hours = int(temp.split(':')[0])
        minutes = int(temp.split(':')[1])
        dateBlock = targetDt + dt.timedelta(hours= hours, minutes= minutes)
        dateTimeList.append(dateBlock)

    gujDayAheadDcDf['date_time'] = dateTimeList
    gujDayAheadDcDf = gujDayAheadDcDf.melt(id_vars=['date_time'], value_name='dc_data', var_name= 'unit_name')
    gujDayAheadDcDf['dc_data'] = gujDayAheadDcDf['dc_data'].round()
    gujDayAheadDcDf['plant_name'] = 'xxx'
    gujDayAheadDcDf['plant_id'] = 0

    for i in range(len(gujDayAheadDcDf)):
        gujDayAheadDcDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['day_ahead_dc_file_tag'] == gujDayAheadDcDf.loc[i, "unit_name"]]['plant_name'].values[0]
        gujDayAheadDcDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['day_ahead_dc_file_tag'] == gujDayAheadDcDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    gujDayAheadDcDf = gujDayAheadDcDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    gujDayAheadDcRecords = gujDayAheadDcDf.to_dict('records')

    return gujDayAheadDcRecords
