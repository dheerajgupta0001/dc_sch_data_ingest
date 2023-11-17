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

    # measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    mhIntradayData = pd.read_csv(targetFilePath, nrows= 2)
    blockStr = mhIntradayData['F'][1]
    block_number = int(mhIntradayData['F'][1].split()[2].split("'")[1])
    block_number_2 = block_number+1
    block_number_3 = block_number+2
    block_number_4 = block_number+3
    dcColumnStr = "schedule for '{}' blk(MW)".format(block_number)
    schColumnStr1 = "schedule for '{}' blk(MW)".format(block_number)
    schColumnStr2 = "schedule for '{}' blk(MW)".format(block_number_2)
    schColumnStr3 = "schedule for '{}' blk(MW)".format(block_number_3)
    schColumnStr4 = "schedule for '{}' blk(MW)".format(block_number_4)
    mhIntradayDataDf = pd.read_csv(targetFilePath, skiprows= 2)
    matchingUnitNamesList = []
    if 'Unnamed: 1' in mhIntradayDataDf.columns:
        mhIntradayDataDf.rename(columns = {'Unnamed: 1':'intraday_sch_file_tag'}, inplace = True)
        matchingUnitNamesList.append('intraday_sch_file_tag')
    if schColumnStr1 in mhIntradayDataDf.columns:
        mhIntradayDataDf.rename(columns = {schColumnStr1: 'sch_data_1'}, inplace = True)
        matchingUnitNamesList.append('sch_data_1')
    if schColumnStr2 in mhIntradayDataDf.columns:
        mhIntradayDataDf.rename(columns = {schColumnStr2: 'sch_data_2'}, inplace = True)
        matchingUnitNamesList.append('sch_data_2')
    if schColumnStr3 in mhIntradayDataDf.columns:
        mhIntradayDataDf.rename(columns = {schColumnStr3: 'sch_data_3'}, inplace = True)
        matchingUnitNamesList.append('sch_data_3')
    if schColumnStr4 in mhIntradayDataDf.columns:
        mhIntradayDataDf.rename(columns = {schColumnStr4: 'sch_data_4'}, inplace = True)
        matchingUnitNamesList.append('sch_data_4')


    # matchingUnitNamesList = ['intraday_sch_file_tag', 'sch_data']

    mhIntradayDataDf =  mhIntradayDataDf[matchingUnitNamesList]
    mhIntradayDataDf['block_number'] =  block_number

    mhIntradayDcDf = pd.DataFrame(columns=['intraday_sch_file_tag', 'block_number', 'sch_data_1', 'sch_data_2', 'sch_data_3', 'sch_data_4'])

    for unit in unitNamesList:
        for index, row in mhIntradayDataDf.iterrows():
            if unit == row['intraday_sch_file_tag']:
                matchingUnitList = []
                matchingUnitList.append(mhIntradayDataDf['intraday_sch_file_tag'][index])
                matchingUnitList.append(mhIntradayDataDf['block_number'][index])
                matchingUnitList.append(mhIntradayDataDf['sch_data_1'][index])
                matchingUnitList.append(mhIntradayDataDf['sch_data_2'][index])
                matchingUnitList.append(mhIntradayDataDf['sch_data_3'][index])
                matchingUnitList.append(mhIntradayDataDf['sch_data_4'][index])
                mhIntradayDcDf.loc[len(mhIntradayDcDf)] = matchingUnitList
    # current block
    hours_1 = int(block_number/4)
    minutes_1 = (block_number%4 -1)*15
    dateBlock = targetDt + dt.timedelta(hours= hours_1, minutes= minutes_1)
    block_1_Df = mhIntradayDcDf[['intraday_sch_file_tag', 'sch_data_1']]
    block_1_Df = block_1_Df.rename(columns={"sch_data_1": "sch_data"})
    block_1_Df['date_time'] = dateBlock
    # current + 1 block
    hours_2 = int(block_number_2/4)
    minutes_2 = (block_number_2%4 -1)*15
    dateBlock_2 = targetDt + dt.timedelta(hours= hours_2, minutes= minutes_2)
    block_2_Df = mhIntradayDcDf[['intraday_sch_file_tag', 'sch_data_2']]
    block_2_Df = block_2_Df.rename(columns={"sch_data_2": "sch_data"})
    block_2_Df['date_time'] = dateBlock_2
    # current + 2 block
    hours_3 = int(block_number_3/4)
    minutes_3 = (block_number_3%4 -1)*15
    dateBlock_3 = targetDt + dt.timedelta(hours= hours_3, minutes= minutes_3)
    block_3_Df = mhIntradayDcDf[['intraday_sch_file_tag', 'sch_data_3']]
    block_3_Df = block_3_Df.rename(columns={"sch_data_3": "sch_data"})
    block_3_Df['date_time'] = dateBlock_3
    # current + 3 block
    hours_4 = int(block_number_4/4)
    minutes_4 = (block_number_4%4 -1)*15
    dateBlock_4 = targetDt + dt.timedelta(hours= hours_4, minutes= minutes_4)
    block_4_Df = mhIntradayDcDf[['intraday_sch_file_tag', 'sch_data_4']]
    block_4_Df = block_1_Df.rename(columns={"sch_data_4": "sch_data"})
    block_4_Df['date_time'] = dateBlock_4
    mhIntradaySchDf = pd.concat([block_1_Df, block_2_Df, block_3_Df, block_4_Df])
    mhIntradaySchDf = mhIntradaySchDf.reset_index(drop=True)

    mhIntradaySchDf['sch_data'] = pd.to_numeric(mhIntradaySchDf['sch_data'], errors='coerce')
    mhIntradaySchDf['sch_data'] = mhIntradaySchDf['sch_data'].round()
    mhIntradaySchDf['plant_name'] = 'xxx'
    mhIntradaySchDf['plant_id'] = 0
    mhIntradaySchDf.rename(columns = {'intraday_sch_file_tag':'unit_name'}, inplace = True)


    for i in range(len(mhIntradaySchDf)):
        mhIntradaySchDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['intraday_sch_file_tag'] == mhIntradaySchDf.loc[i, "unit_name"]]['plant_name'].values[0]
        mhIntradaySchDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['intraday_sch_file_tag'] == mhIntradaySchDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    mhIntradaySchDf = mhIntradaySchDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    mhIntradaySchRecords = mhIntradaySchDf.to_dict('records')

    return mhIntradaySchRecords
