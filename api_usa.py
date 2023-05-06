from urllib.request import urlopen
import pandas as pd

url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&orderby=time"
df_usa= pd.read_csv(url)
pd.set_option('display.max_columns', None)

df_usa= df_usa.drop(columns=['magType', 'nst', 'gap', 'dmin', 'rms', 'net', 'id','updated','type', 'horizontalError', 'depthError', 'magError', 'magNst', 'status', 'locationSource', 'magSource'])

df_usa