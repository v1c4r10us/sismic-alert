from urllib.request import urlopen
import json as json
import pandas as pd
import numpy as np
import requests

#               ETL USA

#importamos la informacion a traves de la API
url = "https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&orderby=time"
df_usa= pd.read_csv(url)
pd.set_option('display.max_columns', None)

def transformacion_usa(df):
    #elimino columnas innecesarias
    df= df.drop(columns=['magType', 'nst', 'gap', 'dmin', 'rms', 'net', 'id','updated','type', 'horizontalError', 'depthError', 'magError', 'magNst', 'status', 'locationSource', 'magSource'])
    #coloco todos los registros en minusculas
    df= df.applymap(lambda x: x.lower() if isinstance(x, str) else x)
    #Vamos a redondear las columnas de variables float
    df[['latitude','longitude','depth','mag']]=df[['latitude','longitude','depth','mag']].round(1)
    #eliminamos duplicados
    df= df.drop_duplicates()
    #agrego una columna "country" con el nombre del pais respectivo en caso que necesite identificar en procesos posteriores
    df['country']='usa'

    #cambiamos el formato de fecha para estandarizarla junto con los otros datasets
    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df


#           ETL JAPON

#importamos los datos
url = "https://www.jma.go.jp/bosai/quake/data/list.json"
response = requests.get(url)
data = response.json()
df_japon = pd.DataFrame(data)

def transformacion_japon(df):
    #eliminamos las columnas que no vamos a utilizar
    df= df.drop(columns=['ctt','eid','rdt','ttl','ift','ser','anm','acd','maxi','int','json','en_ttl'])
    #eliminamos y/o reemplazamos caracateres innecesarios
    df['cod'] = df['cod'].str.replace('+', '', 1) # Esta linea de codigo reemplaza exclusivamente el primer '+'
    df['cod'] = df['cod'].str.replace('+', ',')
    df['cod'] = df['cod'].str.replace('-', ',')
    df['cod'] = df['cod'].str.replace('/', '')

    #ahora que esta limpio podemos separar los datos
    df = df.join(df['cod'].str.split(',', expand=True).rename(columns={0:'latitude', 1:'longitude', 2:'depth'}))
    df = df.drop(columns='cod')

    #convertimos a float la columna "depth"
    df['depth'] = df['depth'].astype('float64')
    #dividimos por mil para llevar la unidad de medida a KM para mantener la misma en todos los datasets
    df['depth'] = (df['depth'] / 1000)

    #renombramos columnas
    df = df.rename(columns={'at': 'time', 'en_anm': 'place'})
    #reordenamos columnas
    orden = ['time', 'latitude', 'longitude', 'depth', 'mag', 'place']
    df = df[orden]

    #todo a minusculas
    df=df.applymap(lambda x: x.lower() if isinstance(x,str) else x)
    #agrego una columna "country" con el nombre del pais respectivo en caso que necesite identificar en procesos posteriores
    df['country']='japon'

    #Vamos a redondear las columnas de variables float
    df[['latitude','longitude','depth','mag']]=df[['latitude','longitude','depth','mag']].round(1)

    #reemplazamos los "ｍ不明" ("desconocido en español") de la columna "mag" por NaN
    df['mag'] = df['mag'].replace({'ｍ不明': np.nan, '': np.nan})

    #eliminamos duplicados
    df= df.drop_duplicates()
    
    #substituimos los Nones por NaN
    df['longitude'] = df['longitude'].replace({None: np.nan, '': np.nan})
    df['latitude'] = df['latitude'].replace({None: np.nan, '': np.nan})

    #cambiamos el formato de fecha para estandarizarla junto con los otros datasets
    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')

    #elimino los registos nulos pero solo aquellos que son nulos en todas las columnas
    df = df.dropna(subset=['latitude', 'longitude', 'depth', 'mag','place'], how='all')
    
    return df


#           ETL CHILE

#traemos la informacion a traves de la API
url = "https://api.xor.cl/sismo/recent"

response = requests.get(url)
data = response.json()
df_chile = pd.DataFrame(data)
df_chile

def transformacion_chile(df):
    #Elminamos las columnas innecesarias y nos quedamos solo con la columna "events"
    df= df.drop(columns=['status_code','status_description'])

    #Extraemos la informacion y la adjuntamos al dataframe como nuevas columnas
    df['time']= df['events'].apply(lambda x : x['utc_date'])
    df['latitude']= df['events'].apply(lambda x : x['latitude'])
    df['longitude']=df['events'].apply(lambda x: x['longitude'])
    df['depth']=df['events'].apply(lambda x: x['depth'])
    df['mag']=df['events'].apply(lambda x: x['magnitude']['value'])
    df['place']=df['events'].apply(lambda x: x['geo_reference'])

    #Eliminamos la columna "events", ahora ya no la necesitamos
    df= df.drop(columns=['events'])

    #Pasamos todo a minusculas
    df= df.applymap(lambda x : x.lower() if isinstance(x,str) else x)
    #Agrego una columna "country" con el nombre del pais respectivo en caso que necesite identificar en procesos posteriores
    df['country']= 'chile'

    #convertimos el tipo de la columna "depth" a float para mantener la misma estructura en todos los datasets
    df['depth']= df['depth'].astype('Float64')
    #eliminamos duplicados
    df= df.drop_duplicates()
    #Vamos a redondear las columnas de variables float
    df[['latitude','longitude','depth','mag']]=df[['latitude','longitude','depth','mag']].round(1)

    #cambiamos el formato de fecha para estandarizarla junto con los otros datasets
    df['time'] = pd.to_datetime(df['time'])
    df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return df



#           TRANSFORMACIONES

df_usa= transformacion_usa(df_usa)
df_japon= transformacion_japon(df_japon)
df_chile= transformacion_chile(df_chile)

#           CONCATENAR

df_final= pd.concat([df_usa,df_japon,df_chile])