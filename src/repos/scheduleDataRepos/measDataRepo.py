from typing import List
from src.repos.scheduleDataRepos.insertChattIntradaySchMetricData import insertChattIntradaySchData
from src.repos.scheduleDataRepos.insertMpIntradaySchMetricData import insertMpIntradaySchData
from src.repos.scheduleDataRepos.insertMhIntradaySchMetricData import insertMhIntradaySchData
from src.repos.scheduleDataRepos.insertGujIntradaySchMetricData import insertGujIntradaySchData
from src.repos.scheduleDataRepos.getUnitNameForState import getUnitNameForState
from src.typeDefs.scheduleTypeRecord.chattIntradaySchRecord import IChattIntradaySchDataRecord
from src.typeDefs.scheduleTypeRecord.mpIntradaySchRecord import IMpIntradaySchDataRecord
from src.typeDefs.scheduleTypeRecord.mhIntradaySchRecord import IMhIntradaySchDataRecord
from src.typeDefs.scheduleTypeRecord.gujIntradaySchRecord import IGujIntradaySchDataRecord


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
    
    def insertChattIntradaySchData(self, dataSamples: List[IChattIntradaySchDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertChattIntradaySchData(dataSamples)
    
    def insertMpIntradaySchData(self, dataSamples: List[IMpIntradaySchDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertMpIntradaySchData(dataSamples)
    
    def insertMhIntradaySchData(self, dataSamples: List[IMhIntradaySchDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertMhIntradaySchData(dataSamples)
    
    def insertGujIntradaySchData(self, dataSamples: List[IGujIntradaySchDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertGujIntradaySchData(dataSamples)
