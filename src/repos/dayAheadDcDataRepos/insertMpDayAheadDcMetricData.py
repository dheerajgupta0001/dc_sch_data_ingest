from typing_extensions import final
import psycopg2
from typing import List
import datetime as dt
from src.config.appConfig import initConfigs, getJsonConfig
from src.typeDefs.dayAheadDcTypeRecord.mpDayAheadDcRecord import IMpDayAheadDcDataRecord

initConfigs()
dbConfig = getJsonConfig()


def insertMpDayAheadDcData(dataRows: List[IMpDayAheadDcDataRecord]) -> bool:
    """Inserts a entity metrics time series data into the app db

    Args:
    appDbConnStr (str): [description]
    dataSamples (List[IMetricsDataRecord]): [description]

    Returns:
        bool: returns true if process is ok
    """
    # TODO
    dbConn = None
    dbCur = None
    isInsertSuccess = True
    try:
        dbConn = psycopg2.connect(host=dbConfig['db_host'], dbname=dbConfig['db_name'],
                                  user=dbConfig['db_username'], password=dbConfig['db_password'])
        dbCur = dbConn.cursor()
        # insert the raw data
        rowIter = 0
        insIncr = 500
        numRows = len(dataRows)
        while rowIter < numRows:
            # set iteration values
            iteratorEndVal = rowIter+insIncr
            if iteratorEndVal >= numRows:
                iteratorEndVal = numRows

            # Create row tuples
            dataInsertionTuples = []
            for insRowIter in range(rowIter, iteratorEndVal):
                dataRow = dataRows[insRowIter]

                dataInsertionTuple = (dt.datetime.strftime(dataRow['date_time'], '%Y-%m-%d %H:%M:%S'), dataRow['plant_name'],
                                      dataRow['dc_data'], dataRow['plant_id'])
                dataInsertionTuples.append(dataInsertionTuple)

            # prepare sql for insertion and execute
            dataText = ','.join(dbCur.mogrify('(%s,%s,%s,%s)', row).decode(
                "utf-8") for row in dataInsertionTuples)
            sqlTxt = 'INSERT INTO public.intraday_dc_data(\
        	date_time, plant_name, dc_data, plant_id)\
        	VALUES {0} on conflict (date_time, plant_id) \
            do update set dc_data = excluded.dc_data'.format(dataText)
            dbCur.execute(sqlTxt)
            dbConn.commit()

            rowIter = iteratorEndVal

        # close cursor and connection

    except Exception as err:
        isInsertSuccess = False
        print('Error while inserting unit name for {} from master table'.format())
        print(err)

    finally:
        if dbCur is not None:
            dbCur.close()
        if dbConn is not None:
            dbConn.close()

    return isInsertSuccess
