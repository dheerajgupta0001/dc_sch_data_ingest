from typing import List
from src.repos.insertGujIntradayDcMetricData import insertGujIntradayDcData
from src.repos.insertChattIntradayDcMetricData import insertChattIntradayDcData
from src.repos.insertMpIntradayDcMetricData import insertMpIntradayDcData
from src.repos.insertMhIntradayDcMetricData import insertMhIntradayDcData
from src.repos.getUnitNameForState import getUnitNameForState
from src.typeDefs.gujIntradayDcRecord import IGujIntradayDcDataRecord
from src.typeDefs.chattIntradayDcRecord import IChattIntradayDcDataRecord
from src.typeDefs.mpIntradayDcRecord import IMpIntradayDcDataRecord
from src.typeDefs.mhIntradayDcRecord import IMhIntradayDcDataRecord


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

    def insertGujIntradayDcData(self, dataSamples: List[IGujIntradayDcDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertGujIntradayDcData(dataSamples)
    
    def insertChattIntradayDcData(self, dataSamples: List[IChattIntradayDcDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertChattIntradayDcData(dataSamples)
    
    def insertMpIntradayDcData(self, dataSamples: List[IMpIntradayDcDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertMpIntradayDcData(dataSamples)
    
    def insertMhIntradayDcData(self, dataSamples: List[IMhIntradayDcDataRecord]) -> bool:
        """inserts a entity metrics time series data into the app db
        Returns:
            bool: returns true if process is ok
        """
        return insertMhIntradayDcData(dataSamples)
