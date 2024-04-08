from typing import List
from src.repos.scheduleDataRepos.insertChattIntradaySchMetricData import insertChattIntradaySchData
from src.repos.scheduleDataRepos.insertMpIntradaySchMetricData import insertMpIntradaySchData
from src.repos.scheduleDataRepos.insertMhIntradaySchMetricData import insertMhIntradaySchData
from src.repos.scheduleDataRepos.insertGujIntradaySchMetricData import insertGujIntradaySchData
from src.repos.dayAheadDcDataRepos.getUnitNameForState import getUnitNameForState
from src.typeDefs.dayAheadDcTypeRecord.chattDayAheadDcRecord import IChattDayAheadDcDataRecord
from src.typeDefs.dayAheadDcTypeRecord.mpDayAheadDcRecord import IMpDayAheadDcDataRecord
from src.typeDefs.dayAheadDcTypeRecord.mhDayAheadDcRecord import IMhDayAheadDcDataRecord
from src.typeDefs.dayAheadDcTypeRecord.gujDayAheadDcRecord import IGujDayAheadDcDataRecord


class MeasDataRepo():
    """Repository class for entity metrics data
    """
    appDbConnStr: str = ""

    def __init__(self, dbConStr: str) -> None:
        """constructor method
        Args:
            dbConStr (str): database connection string
        """
        self.appDbConnStr = dbConStr

    def getUnitNameForState(self, stateName: str) -> List:
        """_summary_

        Args:
            stateName (str): _description_

        Returns:
            List: List of unit name from database for that state
        """
        return getUnitNameForState(stateName)
    
    def insertChattIntradaySchData(self, dataSamples: List[IChattDayAheadDcDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertChattIntradaySchData(dataSamples)
    
    def insertMpIntradaySchData(self, dataSamples: List[IMpDayAheadDcDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertMpIntradaySchData(dataSamples)
    
    def insertMhIntradaySchData(self, dataSamples: List[IMhDayAheadDcDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertMhIntradaySchData(dataSamples)
    
    def insertGujIntradaySchData(self, dataSamples: List[IGujDayAheadDcDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertGujIntradaySchData(dataSamples)
