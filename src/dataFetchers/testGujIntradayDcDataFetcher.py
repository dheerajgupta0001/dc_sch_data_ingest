import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.gujIntradayDcRecord import IGujIntradayDcDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getGujIntradayDcData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IGujIntradayDcDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for guj state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IGujIntradayDcDataRecord]: List of date(blockwise), unit name & DC data
    """
    gujIntradayDcRecords: List[IGujIntradayDcDataRecord] = []
    unitNamesList = unitDetailsDf['intraday_dc_file_tag'].to_list()

    # separate the comma separated flags
    tempListWithCommaSepValues = [map(lambda x: x.strip(), item.split(',')) for item in unitNamesList]
    unitNamesListWithCommaSep = [item for sub_list in tempListWithCommaSepValues for item in sub_list]

    # check how many entities are clubbed using comma separated in master table
    commaSeparatedList = []
    for i in range(len(unitNamesList)):
        if (unitNamesList[i].count(",") + 1)>1:
            commaSeparatedList.append(unitNamesList[i])

    gujIntradayDataDf = pd.read_csv(
        targetFilePath, nrows= 96)
    
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

    measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    matchingUnitNamesList = []
    for unit in unitNamesList:
        if unit in gujIntradayDataDf.columns:
            matchingUnitNamesList.append(unit)
    gujIntradayDcDf =  gujIntradayDataDf[matchingUnitNamesList]
    hoursMinutes = gujIntradayDataDf.iloc[:, 0]
    dateTimeList = []
    for temp in hoursMinutes:
        hours = int(temp.split(':')[0])
        minutes = int(temp.split(':')[1])
        dateBlock = targetDt + dt.timedelta(hours= hours, minutes= minutes)
        dateTimeList.append(dateBlock)

    gujIntradayDcDf['date_time'] = dateTimeList
    gujIntradayDcDf = gujIntradayDcDf.melt(id_vars=['date_time'], value_name='dc_data', var_name= 'unit_name')
    gujIntradayDcDf['dc_data'] = gujIntradayDcDf['dc_data'].round()
    gujIntradayDcDf['plant_name'] = 'xxx'
    gujIntradayDcDf['plant_id'] = 0

    for i in range(len(gujIntradayDcDf)):
        # gujIntradayDcDf['plant_id'][i] = unitDetailsDf.loc[unitDetailsDf['intraday_dc_file_tag'] == gujIntradayDcDf.loc[i, "unit_name"], 'id']
        # gujIntradayDcDf['plant_name'][i] = unitDetailsDf.loc[unitDetailsDf['intraday_dc_file_tag'] == gujIntradayDcDf.loc[i, "unit_name"], 'plant_name']

        gujIntradayDcDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['intraday_dc_file_tag'] == gujIntradayDcDf.loc[i, "unit_name"]]['plant_name'].values[0]
        gujIntradayDcDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['intraday_dc_file_tag'] == gujIntradayDcDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    gujIntradayDcDf = gujIntradayDcDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    gujIntradayDcRecords = gujIntradayDcDf.to_dict('records')

    return gujIntradayDcRecords
