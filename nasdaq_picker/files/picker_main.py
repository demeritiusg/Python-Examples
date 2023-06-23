import nasdaqdatalink
from datetime import date

nasdaqdatalink.read_key(filename="D:\Git\Python-Examples\\nasdaq_picker\.nasdaq\data_link_apikey")

#data = nasdaqdatalink.get_table('ZACKS/FC', ticker='AAPL')
class NasdaqPicker():
    def __init__(self):
        pass

    def getEconData(self, type:str, start_date:date, end_date:date):
        """
        Function to retrieve economic data
        """
        #data = nasdaqdatalink.get("FRED/GDP", start_date="2001-12-31", end_date="2005-12-31")
        data = nasdaqdatalink.get(type, start_date=start_date, end_date=end_date)

        print(data)

    def getStockData():
        """
        Future function for stock data
        """
        pass