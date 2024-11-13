import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.mhIntradayDcRecord import IMhIntradayDcDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getMhIntradayDcData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IMhIntradayDcDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for mh state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IMhIntradayDcDataRecord]: List of date(blockwise), unit name & Dc data
    """
    mhIntradayDcRecords: List[IMhIntradayDcDataRecord] = []
    unitNamesList = unitDetailsDf['intraday_dc_file_tag'].to_list()

    # separate the comma separated flags
    tempListWithCommaSepValues = [map(lambda x: x.strip(), item.split('$')) for item in unitNamesList]
    unitNamesListWithCommaSep = [item for sub_list in tempListWithCommaSepValues for item in sub_list]

    # check how many entities are clubbed using comma separated in master table
    commaSeparatedList = []
    for i in range(len(unitNamesList)):
        if (unitNamesList[i].count("$") + 1)>1:
            commaSeparatedList.append(unitNamesList[i])

    # mhIntradayDataDf = pd.read_csv(targetFilePath, skiprows=5, usecols=range(98))
    mhIntradayDataDf = pd.read_csv(targetFilePath, skiprows=6, on_bad_lines='skip', header = None, usecols = [1, *range(9,105)])
    mhIntradaySchDataDf = pd.read_csv(targetFilePath, skiprows=6, on_bad_lines='skip', header = None, usecols = [1, *range(106,202)])
    # mhIntradayDataDf = mhIntradayDataDf.loc[:, ~mhIntradayDataDf.columns.str.contains('^Unnamed')]
    mhIntradayDataDf = mhIntradayDataDf.rename(columns={1: 'intraday_dc_file_tag'})
    mhIntradaySchDataDf = mhIntradaySchDataDf.rename(columns={1: 'intraday_dc_file_tag'})
    mhIntradayDataDf = mhIntradayDataDf.melt(id_vars=['intraday_dc_file_tag'], value_name='dc_data', var_name= 'block_number')
    mhIntradaySchDataDf = mhIntradaySchDataDf.melt(id_vars=['intraday_dc_file_tag'], value_name='sch_data', var_name= 'block_number')
    mhIntradayDcDf = pd.DataFrame(columns=['intraday_dc_file_tag', 'block_number', 'dc_data'])
    mhIntradaySchDf = pd.DataFrame(columns=['intraday_dc_file_tag', 'block_number', 'dc_data'])

    for unit in unitNamesListWithCommaSep:
        for index, row in mhIntradayDataDf.iterrows():
            if unit == row['intraday_dc_file_tag']:
                matchingUnitList = []
                matchingUnitList.append(mhIntradayDataDf['intraday_dc_file_tag'][index])
                matchingUnitList.append(mhIntradayDataDf['block_number'][index])
                matchingUnitList.append(mhIntradayDataDf['dc_data'][index])
                if float(mhIntradaySchDataDf['sch_data'][index]) == 0:
                    matchingUnitList[2] = 0
                mhIntradayDcDf.loc[len(mhIntradayDcDf)] = matchingUnitList
                # mhIntradaySchDf.loc[len(mhIntradayDcDf)] = matchingUnitList 

    mhIntradayDcDf = pd.pivot_table(mhIntradayDcDf, values ='dc_data', index =['block_number'],
                         columns =['intraday_dc_file_tag'])
    mhIntradayDcDf = mhIntradayDcDf.reset_index()
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


    mhIntradayDcDf['date_time'] = dateTimeList
    mhIntradayDcDf = mhIntradayDcDf.drop(columns=['block_number'])

    # check matching columns starts
    for temp in commaSeparatedList:
        tempList = temp.split('$')
        matchingList = []
        for unit in tempList:
            if unit in mhIntradayDcDf.columns:
                matchingList.append(unit)
        combinedGasData = mhIntradayDcDf[matchingList]
        mhIntradayDcDf[temp] = combinedGasData.sum(axis=1)
    # check matching columns ends

    matchingUnitNamesList = []
    for unit in unitNamesList:
        if unit in mhIntradayDcDf.columns:
            matchingUnitNamesList.append(unit)
    mhIntradayDcDf =  mhIntradayDcDf[matchingUnitNamesList]

    mhIntradayDcDf['date_time'] = dateTimeList


    mhIntradayDcDf = mhIntradayDcDf.melt(id_vars=['date_time'], value_name='dc_data', var_name= 'unit_name')
    # mhIntradayDcDf['dc_data'] = mhIntradayDcDf['dc_data'].round()
    mhIntradayDcDf['plant_name'] = 'xxx'
    mhIntradayDcDf['plant_id'] = 0

    for i in range(len(mhIntradayDcDf)):
        mhIntradayDcDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['intraday_dc_file_tag'] == mhIntradayDcDf.loc[i, "unit_name"]]['plant_name'].values[0]
        mhIntradayDcDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['intraday_dc_file_tag'] == mhIntradayDcDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    mhIntradayDcDf = mhIntradayDcDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    mhIntradayDcRecords = mhIntradayDcDf.to_dict('records')

    return mhIntradayDcRecords