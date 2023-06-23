import nasdaqdatalink

nasdaqdatalink.read_key(filename="D:\Git\Python-Examples\\nasdaq_picker\.nasdaq\data_link_apikey")

#data = nasdaqdatalink.get_table('ZACKS/FC', ticker='AAPL')
data = nasdaqdatalink.get("FRED/GDP", start_date="2001-12-31", end_date="2005-12-31")

print(data)