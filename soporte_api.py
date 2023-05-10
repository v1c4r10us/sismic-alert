from urllib.request import urlopen
import json as json
import pandas as pd
import numpy as np
import requests

#               ETL USA

#importamos los datos
url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&orderby=time"
df_usa= pd.read_csv(url)
pd.set_option('display.max_columns', None)

#elimino columnas innecesarias
df_usa= df_usa.drop(columns=['magType', 'nst', 'gap', 'dmin', 'rms', 'net', 'id','updated','type', 'horizontalError', 'depthError', 'magError', 'magNst', 'status', 'locationSource', 'magSource'])
#coloco todos los registros en minusculas
df_usa= df_usa.applymap(lambda x: x.lower() if isinstance(x, str) else x)
#Vamos a redondear las columnas de variables float
df_usa[['latitude','longitude','depth','mag']]=df_usa[['latitude','longitude','depth','mag']].round(1)
#eliminamos duplicados
df_usa= df_usa.drop_duplicates()
#agrego una columna "country" con el nombre del pais respectivo en caso que necesite identificar en procesos posteriores
df_usa['country']='usa'

#cambiamos el formato de fecha para estandarizarla junto con los otros datasets
df_usa['time'] = pd.to_datetime(df_usa['time'])
df_usa['time'] = df_usa['time'].dt.strftime('%Y-%m-%d %H:%M:%S')



#           ETL JAPON

#importamos los datos
url = "https://www.jma.go.jp/bosai/quake/data/list.json"
response = requests.get(url)
data = response.json()
df_japon = pd.DataFrame(data)

#eliminamos las columnas que no vamos a utilizar
df_japon= df_japon.drop(columns=['ctt','eid','rdt','ttl','ift','ser','anm','acd','maxi','int','json','en_ttl'])
#eliminamos y/o reemplazamos caracateres innecesarios
df_japon['cod'] = df_japon['cod'].str.replace('+', '', 1) # Esta linea de codigo reemplaza exclusivamente el primer '+'
df_japon['cod'] = df_japon['cod'].str.replace('+', ',')
df_japon['cod'] = df_japon['cod'].str.replace('-', ',')
df_japon['cod'] = df_japon['cod'].str.replace('/', '')

#ahora que esta limpio podemos separar los datos
df_japon = df_japon.join(df_japon['cod'].str.split(',', expand=True).rename(columns={0:'latitude', 1:'longitude', 2:'depth'}))
df_japon = df_japon.drop(columns='cod')

#convertimos a float la columna "depth"
df_japon['depth'] = df_japon['depth'].astype('float64')
#dividimos por mil para llevar la unidad de medida a KM para mantener la misma en todos los datasets
df_japon['depth'] = (df_japon['depth'] / 1000)

#renombramos columnas
df_japon = df_japon.rename(columns={'at': 'time', 'en_anm': 'place'})
#reordenamos columnas
orden = ['time', 'latitude', 'longitude', 'depth', 'mag', 'place']
df_japon = df_japon[orden]

#todo a minusculas
df_japon=df_japon.applymap(lambda x: x.lower() if isinstance(x,str) else x)
#agrego una columna "country" con el nombre del pais respectivo en caso que necesite identificar en procesos posteriores
df_japon['country']='japon'

#reemplazamos los "ｍ不明" ("desconocido en español") de la columna "mag" por NaN
df_japon['mag'] = df_japon['mag'].replace({'ｍ不明': np.nan, '': np.nan})

#eliminamos duplicados
df_japon= df_japon.drop_duplicates()
#Vamos a redondear las columnas de variables float
df_japon[['latitude','longitude','depth','mag']]=df_japon[['latitude','longitude','depth','mag']].round(1)
#substituimos los Nones por NaN
df_japon['longitude'] = df_japon['longitude'].replace({None: np.nan, '': np.nan})
df_japon['latitude'] = df_japon['latitude'].replace({None: np.nan, '': np.nan})

#cambiamos el formato de fecha para estandarizarla junto con los otros datasets
df_japon['time'] = pd.to_datetime(df_japon['time'])
df_japon['time'] = df_japon['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

#elimino los registos nulos pero solo aquellos que son nulos en todas las columnas
df_lower = df_japon.dropna(subset=['latitude', 'longitude', 'depth', 'mag','place'], how='all')



#           ETL CHILE

#traemos la informacion a traves de la API
url = "https://api.xor.cl/sismo/recent"
response = requests.get(url)
data = response.json()
df_chile = pd.DataFrame(data)
df_chile.head()

#Elminamos las columnas innecesarias y nos quedamos solo con la columna "events"
df_chile= df_chile.drop(columns=['status_code','status_description'])

#Extraemos la informacion y la adjuntamos al dataframe como nuevas columnas
df_chile['time']= df_chile['events'].apply(lambda x : x['utc_date'])
df_chile['latitude']= df_chile['events'].apply(lambda x : x['latitude'])
df_chile['longitude']=df_chile['events'].apply(lambda x: x['longitude'])
df_chile['depth']=df_chile['events'].apply(lambda x: x['depth'])
df_chile['mag']=df_chile['events'].apply(lambda x: x['magnitude']['value'])
df_chile['place']=df_chile['events'].apply(lambda x: x['geo_reference'])

#Eliminamos la columna "events", ahora ya no la necesitamos
df_chile= df_chile.drop(columns=['events'])

#Pasamos todo a minusculas
df_chile= df_chile.applymap(lambda x : x.lower() if isinstance(x,str) else x)
#Agrego una columna "country" con el nombre del pais respectivo en caso que necesite identificar en procesos posteriores
df_chile['country']= 'chile'

#convertimos el tipo de la columna "depth" a float para mantener la misma estructura en todos los datasets
df_chile['depth']= df_chile['depth'].astype('Float64')
#eliminamos duplicados
df_chile= df_chile.drop_duplicates()
#Vamos a redondear las columnas de variables float
df_chile[['latitude','longitude','depth','mag']]=df_chile[['latitude','longitude','depth','mag']].round(1)

#cambiamos el formato de fecha para estandarizarla junto con los otros datasets
df_chile['time'] = pd.to_datetime(df_chile['time'])
df_chile['time'] = df_chile['time'].dt.strftime('%Y-%m-%d %H:%M:%S')


#           CONCATENAR LOS 3 DATASETS

df_final= pd.concat([df_usa,df_japon,df_chile])