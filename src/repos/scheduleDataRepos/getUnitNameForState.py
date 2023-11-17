from typing_extensions import final
from typing import List
import psycopg2
from src.config.appConfig import initConfigs, getJsonConfig
import pandas as pd

initConfigs()    
dbConfig = getJsonConfig()


def getUnitNameForState(stateName) -> pd.DataFrame():
    """_summary_

    Args:
        appDbConnStr (str): _description_
        stateName (str): List of unit name for given state from master table

    Returns:
        List: _description_
    """
    dbConn = None
    dbCur = None
    unitList = []
    try:
        dbConn = psycopg2.connect(host=dbConfig['db_host'], dbname=dbConfig['db_name'],
                                     user=dbConfig['db_username'], password=dbConfig['db_password'])
        dbCur = dbConn.cursor()
        # insert the raw data
        sql_fetch = "select id, intraday_sch_file_tag, plant_name from public.mapping_table where state in ('{0}')".format(stateName)
        dbCur.execute(sql_fetch)
        # unitData = dbCur.fetchall()
        unitDf = pd.read_sql(sql_fetch, dbConn)
        unitList = unitDf['intraday_sch_file_tag'].to_list()
        plantList = unitDf['plant_name'].to_list()
        plantId = unitDf['id'].to_list()
        unitDf = unitDf.dropna(subset=['intraday_sch_file_tag'])

    except Exception as err:
        print('Error while fetching unit name for {} from master table'.format(stateName))
        print(err)

    finally:
        if dbCur is not None:
            dbCur.close()
        if dbConn is not None:
            dbConn.close()
    return unitDf
