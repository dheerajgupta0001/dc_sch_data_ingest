import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.scheduleTypeRecord.gujIntradaySchRecord import IGujIntradaySchDataRecord
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getGujIntradaySchData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IGujIntradaySchDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for guj state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IGujIntradaySchDataRecord]: List of date(blockwise), unit name & Sch data
    """
    gujIntradaySchRecords: List[IGujIntradaySchDataRecord] = []
    unitNamesList = unitDetailsDf['intraday_sch_file_tag'].to_list()

    # check how many entities are clubbed using comma separated in master table
    commaSeparatedList = []
    for i in range(len(unitNamesList)):
        if (unitNamesList[i].count(",") + 1)>1:
            commaSeparatedList.append(unitNamesList[i])

    gujIntradayDataDf = pd.read_csv(
        targetFilePath, nrows= 96)
    gujIntradayDataDf = gujIntradayDataDf.iloc[:, 2:]

    # check matching columns starts
    for temp in commaSeparatedList:
        tempList = temp.split(',')
        matchingList = []
        for unit in tempList:
            if unit in gujIntradayDataDf.columns:
                matchingList.append(unit)
        combinedGasData = gujIntradayDataDf[matchingList]
        gujIntradayDataDf[temp] = combinedGasData.sum(axis=1)
    # check matching columns ends
    
    matchingUnitNamesList = []
    for unit in unitNamesList:
        if unit in gujIntradayDataDf.columns:
            matchingUnitNamesList.append(unit)
    gujIntradaySchDf =  gujIntradayDataDf[matchingUnitNamesList]
    gujIntradayDataDf = gujIntradayDataDf.loc[:, ~gujIntradayDataDf.columns.str.contains('^Unnamed')]
    hoursMinutes = gujIntradayDataDf.iloc[:, 0]
    dateTimeList = []
    for temp in hoursMinutes:
        hrsMin = temp.split('-')[0]
        hours = int(hrsMin.split(':')[0])
        minutes = int(hrsMin.split(':')[1])
        dateBlock = targetDt + dt.timedelta(hours= hours, minutes= minutes)
        dateTimeList.append(dateBlock)

    gujIntradaySchDf['date_time'] = dateTimeList
    gujIntradaySchDf = gujIntradaySchDf.melt(id_vars=['date_time'], value_name='sch_data', var_name= 'unit_name')
    gujIntradaySchDf['sch_data'] = gujIntradaySchDf['sch_data'].round()
    gujIntradaySchDf['plant_name'] = 'xxx'
    gujIntradaySchDf['plant_id'] = 0

    for i in range(len(gujIntradaySchDf)):
        # gujIntradaySchDf['plant_id'][i] = unitDetailsDf.loc[unitDetailsDf['intraday_sch_file_tag'] == gujIntradaySchDf.loc[i, "unit_name"], 'id']
        # gujIntradaySchDf['plant_name'][i] = unitDetailsDf.loc[unitDetailsDf['intraday_sch_file_tag'] == gujIntradaySchDf.loc[i, "unit_name"], 'plant_name']

        gujIntradaySchDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['intraday_sch_file_tag'] == gujIntradaySchDf.loc[i, "unit_name"]]['plant_name'].values[0]
        gujIntradaySchDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['intraday_sch_file_tag'] == gujIntradaySchDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    gujIntradaySchDf = gujIntradaySchDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    gujIntradaySchRecords = gujIntradaySchDf.to_dict('records')

    return gujIntradaySchRecords
