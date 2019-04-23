import pandas as pd
import numpy as np

count = '10000000'
df = pd.read_csv('results/'+count+'.csv')

#removes outliers
for val in df[['1','2','3','4','5','6','7','8','9','10']]:
    df[val] = np.where(np.abs((df[val]-df.median(axis=1))/df.std(axis=1)) > 3, np.nan, df[val])

#replaces nan with mean of row
df[['1','2','3','4','5','6','7','8','9','10']] = df[['1','2','3','4','5','6','7','8','9','10']].apply(lambda row: row.fillna(row.mean()), axis=1)
df.to_csv('results/'+count+'__fix.csv', index=False)
