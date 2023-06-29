"""
to generate a report using mostly pandas
"""

import pandas as pd

df = pd.read_csv(r'D:\GIT_REPOs\Python-Examples\Python-Examples\python files\data\african_mobile_data.csv', index_col=0)
#data = df.reset_index(drop=True)
#data.to_csv(r'D:\GIT_REPOs\Python-Examples\Python-Examples\python files\data\african_mobile_data_2.csv', index=False)
print(df)