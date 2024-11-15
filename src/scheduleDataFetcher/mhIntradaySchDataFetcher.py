import imp
from typing import Dict
import pandas as pd
import numpy as np
from src.config.appConfig import getJsonConfig
import datetime as dt
from src.typeDefs.scheduleTypeRecord.mhIntradaySchRecord import IMhIntradaySchDataRecord
from src.repos.measDataRepo import MeasDataRepo
from src.repos.getUnitNameForState import getUnitNameForState
from typing import List


def getMhIntradaySchData(targetFilePath: str, unitDetailsDf: pd.DataFrame(), targetDt: dt.datetime) -> List[IMhIntradaySchDataRecord]:
    """_summary_
    Args:
        targetFilePath (str): target file for mh state from STFP folder
        unitNamesList (List): list of units from database for that state
        targetDt (dt.datetime): Date for which data has to be fetched (preferably today)
    
    Returns:
        List[IMhIntradaySchDataRecord]: List of date(blockwise), unit name & Sch data
    """
    mhIntradaySchRecords: List[IMhIntradaySchDataRecord] = []
    unitNamesList = unitDetailsDf['intraday_sch_file_tag'].to_list()

    # separate the comma separated flags
    tempListWithCommaSepValues = [map(lambda x: x.strip(), item.split('$')) for item in unitNamesList]
    unitNamesListWithCommaSep = [item for sub_list in tempListWithCommaSepValues for item in sub_list]
    
    # check how many entities are clubbed using comma separated in master table
    commaSeparatedList = []
    for i in range(len(unitNamesList)):
        if (unitNamesList[i].count("$") + 1)>1:
            commaSeparatedList.append(unitNamesList[i])

    # mhIntradayDataDf = pd.read_csv(targetFilePath, skiprows=5, usecols=range(98))
    mhIntradayDataDf = pd.read_csv(targetFilePath, skiprows=6, on_bad_lines='skip', header = None, usecols = [1, *range(106,202)])
    # mhIntradayDataDf = mhIntradayDataDf.loc[:, ~mhIntradayDataDf.columns.str.contains('^Unnamed')]
    mhIntradayDataDf = mhIntradayDataDf.rename(columns={1: 'intraday_sch_file_tag'})
    mhIntradayDataDf = mhIntradayDataDf.melt(id_vars=['intraday_sch_file_tag'], value_name='sch_data', var_name= 'block_number')
    mhIntradaySchDf = pd.DataFrame(columns=['intraday_sch_file_tag', 'block_number', 'sch_data'])

    # for unit in unitNamesListWithCommaSep:
    #     for index, row in mhIntradayDataDf.iterrows():
    #         if unit == row['intraday_sch_file_tag']:
    #             matchingUnitList = []
    #             matchingUnitList.append(mhIntradayDataDf['intraday_sch_file_tag'][index])
    #             matchingUnitList.append(mhIntradayDataDf['block_number'][index])
    #             matchingUnitList.append(mhIntradayDataDf['sch_data'][index])
    #             mhIntradaySchDf.loc[len(mhIntradaySchDf)] = matchingUnitList

    # test starts
    mask = mhIntradayDataDf['intraday_sch_file_tag'].isin(unitNamesListWithCommaSep)
    # Filter DataFrame using mask and select only required columns
    result_df = mhIntradayDataDf[mask][['intraday_sch_file_tag', 'block_number', 'sch_data']].copy()
    # Reset index to ensure continuous indexing
    result_df.reset_index(drop=True, inplace=True)
    # test ends

    mhIntradaySchDf = pd.pivot_table(result_df, values ='sch_data', index =['block_number'],
                         columns =['intraday_sch_file_tag'])
    mhIntradaySchDf = mhIntradaySchDf.reset_index()
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


    mhIntradaySchDf['date_time'] = dateTimeList
    mhIntradaySchDf = mhIntradaySchDf.drop(columns=['block_number'])

    # check matching columns starts
    for temp in commaSeparatedList:
        tempList = temp.split('$')
        matchingList = []
        for unit in tempList:
            if unit in mhIntradaySchDf.columns:
                matchingList.append(unit)
        combinedGasData = mhIntradaySchDf[matchingList]
        mhIntradaySchDf[temp] = combinedGasData.sum(axis=1)
    # check matching columns ends

    matchingUnitNamesList = []
    for unit in unitNamesList:
        if unit in mhIntradaySchDf.columns:
            matchingUnitNamesList.append(unit)
    mhIntradaySchDf =  mhIntradaySchDf[matchingUnitNamesList]

    mhIntradaySchDf['date_time'] = dateTimeList

    
    mhIntradaySchDf = mhIntradaySchDf.melt(id_vars=['date_time'], value_name='sch_data', var_name= 'unit_name')
    # mhIntradaySchDf['sch_data'] = mhIntradaySchDf['sch_data'].round()
    mhIntradaySchDf['plant_name'] = 'xxx'
    mhIntradaySchDf['plant_id'] = 0

    for i in range(len(mhIntradaySchDf)):
        mhIntradaySchDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['intraday_sch_file_tag'] == mhIntradaySchDf.loc[i, "unit_name"]]['plant_name'].values[0]
        mhIntradaySchDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['intraday_sch_file_tag'] == mhIntradaySchDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    mhIntradaySchDf = mhIntradaySchDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    mhIntradaySchRecords = mhIntradaySchDf.to_dict('records')

    return mhIntradaySchRecords