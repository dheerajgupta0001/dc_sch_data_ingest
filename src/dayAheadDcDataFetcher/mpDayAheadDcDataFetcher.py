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
    mpIntradaySchRecords: List[IMpDayAheadDcDataRecord] = []
    unitNamesList = unitDetailsDf['intraday_sch_file_tag'].to_list()

    # measDataRepo = MeasDataRepo(getJsonConfig()['appDbConnStr'])
    # usecols='A:C, F, G:J'
    mpIntradayDataDf = pd.read_csv(targetFilePath, skiprows=5, usecols=range(98))
    if 'Unnamed: 1' in mpIntradayDataDf.columns:
        mpIntradayDataDf = mpIntradayDataDf.drop(columns=['Unnamed: 1'])
    if 'Unnamed: 98' in mpIntradayDataDf.columns:
        mpIntradayDataDf = mpIntradayDataDf.drop(columns=['Unnamed: 98'])

    mpIntradayDataDf.rename(columns = {'Block Interval':'intraday_sch_file_tag'}, inplace = True)
    mpIntradayDataDf = mpIntradayDataDf.melt(id_vars=['intraday_sch_file_tag'], value_name='sch_data', var_name= 'block_number')
    mpIntradaySchDf = pd.DataFrame(columns=['intraday_sch_file_tag', 'block_number', 'sch_data'])

    for unit in unitNamesList:
        for index, row in mpIntradayDataDf.iterrows():
            if unit == row['intraday_sch_file_tag']:
                matchingUnitList = []
                matchingUnitList.append(mpIntradayDataDf['intraday_sch_file_tag'][index])
                matchingUnitList.append(mpIntradayDataDf['block_number'][index])
                matchingUnitList.append(mpIntradayDataDf['sch_data'][index])
                mpIntradaySchDf.loc[len(mpIntradaySchDf)] = matchingUnitList

    mpIntradaySchDf = pd.pivot_table(mpIntradaySchDf, values ='sch_data', index =['block_number'],
                         columns =['intraday_sch_file_tag'])
    mpIntradaySchDf = mpIntradaySchDf.reset_index()
    dateTimeList = []
    hoursMinutes = mpIntradaySchDf.iloc[:, 0]
    dateTimeList = []
    for temp in hoursMinutes:
        blockDetail = temp.split('-')[0]
        hours = int(blockDetail.split(':')[0])
        minutes = int(blockDetail.split(':')[1])
        dateBlock = targetDt + dt.timedelta(hours= hours, minutes= minutes)
        dateTimeList.append(dateBlock)


    mpIntradaySchDf['date_time'] = dateTimeList
    mpIntradaySchDf = mpIntradaySchDf.drop(columns=['block_number'])
    mpIntradaySchDf = mpIntradaySchDf.melt(id_vars=['date_time'], value_name='sch_data', var_name= 'unit_name')
    mpIntradaySchDf['sch_data'] = mpIntradaySchDf['sch_data'].round()
    mpIntradaySchDf['plant_name'] = 'xxx'
    mpIntradaySchDf['plant_id'] = 0

    for i in range(len(mpIntradaySchDf)):
        mpIntradaySchDf.loc[i, 'plant_name'] = unitDetailsDf[unitDetailsDf['intraday_sch_file_tag'] == mpIntradaySchDf.loc[i, "unit_name"]]['plant_name'].values[0]
        mpIntradaySchDf.loc[i, 'plant_id'] = unitDetailsDf[unitDetailsDf['intraday_sch_file_tag'] == mpIntradaySchDf.loc[i, "unit_name"]]['id'].values[0]

    # Remove column name 'unit_name'
    mpIntradaySchDf = mpIntradaySchDf.drop(['unit_name'], axis=1)
    # convert dataframe to list of dictionaries
    mpIntradaySchRecords = mpIntradaySchDf.to_dict('records')

    return mpIntradaySchRecords
