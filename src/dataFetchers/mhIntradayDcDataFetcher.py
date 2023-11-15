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
        List[IMhIntradayDcDataRecord]: List of date(blockwise), unit name & DC data
    """
    mhIntradayDcRecords: List[IMhIntradayDcDataRecord] = []
    unitNamesList = unitDetailsDf['intraday_dc_file_tag'].to_list()

    # measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    mhIntradayData = pd.read_csv(targetFilePath, nrows= 2)
    blockStr = mhIntradayData['F'][1]
    block_number = int(mhIntradayData['F'][1].split()[2].split("'")[1])
    dcColumnStr = "DC for '{}' blk(MW)".format(block_number)
    mhIntradayDataDf = pd.read_csv(targetFilePath, skiprows= 2)
    if 'Unnamed: 1' in mhIntradayDataDf.columns:
        mhIntradayDataDf.rename(columns = {'Unnamed: 1':'intraday_dc_file_tag'}, inplace = True)
    if dcColumnStr in mhIntradayDataDf.columns:
        mhIntradayDataDf.rename(columns = {dcColumnStr: 'dc_data'}, inplace = True)
    matchingUnitNamesList = ['intraday_dc_file_tag', 'dc_data']

    mhIntradayDataDf =  mhIntradayDataDf[matchingUnitNamesList]
    mhIntradayDataDf['block_number'] =  block_number

    mhIntradayDcDf = pd.DataFrame(columns=['intraday_dc_file_tag', 'block_number', 'dc_data'])

    for unit in unitNamesList:
        for index, row in mhIntradayDataDf.iterrows():
            if unit == row['intraday_dc_file_tag']:
                matchingUnitList = []
                matchingUnitList.append(mhIntradayDataDf['intraday_dc_file_tag'][index])
                matchingUnitList.append(mhIntradayDataDf['block_number'][index])
                matchingUnitList.append(mhIntradayDataDf['dc_data'][index])
                mhIntradayDcDf.loc[len(mhIntradayDcDf)] = matchingUnitList
    hours = int(block_number/4)
    minutes = (block_number%4 -1)*15
    dateBlock = targetDt + dt.timedelta(hours= hours, minutes= minutes)


    mhIntradayDcDf['date_time'] = dateBlock
    mhIntradayDcDf = mhIntradayDcDf.drop(columns=['block_number'])
    mhIntradayDcDf['dc_data'] = pd.to_numeric(mhIntradayDcDf['dc_data'], errors='coerce')
    mhIntradayDcDf['dc_data'] = mhIntradayDcDf['dc_data'].round()
    mhIntradayDcDf['plant_name'] = 'xxx'
    mhIntradayDcDf['plant_id'] = 0
    mhIntradayDcDf.rename(columns = {'intraday_dc_file_tag':'unit_name'}, inplace = True)


    for i in range(len(mhIntradayDcDf)):
        mhIntradayDcDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['intraday_dc_file_tag'] == mhIntradayDcDf.loc[i, "unit_name"]]['plant_name'].values[0]
        mhIntradayDcDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['intraday_dc_file_tag'] == mhIntradayDcDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    mhIntradayDcDf = mhIntradayDcDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    mhIntradayDcRecords = mhIntradayDcDf.to_dict('records')

    return mhIntradayDcRecords
