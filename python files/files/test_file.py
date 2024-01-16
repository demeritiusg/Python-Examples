import string
import numpy as np
import pandas as pd
from datetime import datetime

"""
playing with pandas styling
"""

#alist = list(string.ascii_uppercase)
# alist.extend(('AA', 'AB', 'AC', 'AD', 'AE'))

#print(alist)


# df = pd.DataFrame({"col1":[np.nan,3,4,np.nan], "col2":[1,np.nan,45,3]})
# df = df.stack().unique().tolist()
# print(df)

# today = datetime.today().strftime('%x')
# print(today)

df = pd.DataFrame({
    "A": [0, -5, 12, -4, 3],
    "B": [12.24, 3.14, 2.71, -3.14, np.nan],
    "C": [0.5, 1.2, 0.3, 1.9, 2.2],
    "D": [2000, np.nan, 1000, 7000, 5000],
    "E": ['ford', 'telsa', 'gm', 'rvn', 'amc']
})

properties = {"border": "2px solid gray", "color": "green", "font-size": "16px"}
df.style.format({
    "A": "{:.2f}",
    "B": "{:,.5f}",
    "C": "{:.1f}",
    "D": "$ {:,.2f}"
})
#df.to_excel('styled.xlsx', engine='openpyxl')

df.groupby(["E"]).mean()

print(df)
