import nasdaqdatalink
from datetime import date
import requests

nasdaqdatalink.read_key(filename="D:\Git\Python-Examples\\nasdaq_picker\.nasdaq\data_link_apikey")



#data = nasdaqdatalink.get_table('ZACKS/FC', ticker='AAPL')
class NasdaqPicker():

    r = requests.get(url="https://data.nasdaq,com/api/v3/datasets/FED/SUBLPDMHS_XWB_N_Q?start_date=2023-03-31&end_date=2023-09-30&api_key=oiyxctSY4xfCHxyLiL8s")

    def __init__(self):
        self.r

    def getEconData(self, type:str, start_date:date, end_date:date):
        """
        Function to retrieve economic data
        """
        #data = nasdaqdatalink.get("FRED/GDP", start_date="2001-12-31", end_date="2005-12-31")
        data = nasdaqdatalink.get(type, start_date=start_date, end_date=end_date)

        print(data)

    def getgainerslosers():
        """
        Future function for stock data
        """
        # needs to find the top 5 gainers and bottom 10 losers on the daily session 
        pass

    def getmarketbreathe():
        """
        Future function for stock data
        """
        # looking for bookmap, liquity spx, major index
        pass

    def corpbonds(r):
        """
        corp bonds
        """
        

        pass


    def getstockdata():
        """
        order book,
        """