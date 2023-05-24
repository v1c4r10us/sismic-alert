# Intro
El proyecto consiste en implementar una base de datos no relacional (NoSQL), que almacene la información de los movimientos sísmicos que estan ocurriendo en tiempo real y que se encuentran en constante ingesta a los servidores de los organismos geofísicos de USA, Japón y Chile. De esta forma mediante un proceso de extracción de información a través de la interacción directa con las APIs, vamos haciendo consultas iteradas en ciclos de **60 segundos** y posteriormente realizando la limpieza la información para su posterior carga en una plataforma **DBaaS (DataBase as a Service)**. De esta forma se cierra el pipeline de data-engineering.

# Arquitectura
<a href="https://lh3.googleusercontent.com/drive-viewer/AFGJ81qlE-u9qQl5L2Kdb4gLaQNaGXFj35sLhpBLULCoqA3IlCamq8PJypnfLq_i9UMOUZC1Yo6ZHAQQLiVHmZBMNO3crjutZw=s1600?source=screenshot.guru"> <img src="https://lh3.googleusercontent.com/drive-viewer/AFGJ81qlE-u9qQl5L2Kdb4gLaQNaGXFj35sLhpBLULCoqA3IlCamq8PJypnfLq_i9UMOUZC1Yo6ZHAQQLiVHmZBMNO3crjutZw=s1600" /> </a>

# Esquema de almacenamiento en MongoDB Atlas
```json
{
    'id'        :   'Id asignado por MongoDB',
    'time'      :   'fecha y hora del sismo',
    'latitude'  :   'latitud',
    'longitude' :   'longitud',
    'depth'     :   'profundidad',
    'mag'       :   'magnitud',
    'place'     :   'epicentro',
    'country'   :   'país'
}
```

# Microservicio Railway
A través de este microservicio desplegado en Railway como container docker, se ejecuta el fichero **app.py** en intervalos de tiempo prefijados por el fichero con permisos de ejecución **cronworker.sh** del sistema base ubuntu sobre el cual se ejecuta el container, de esta forma se mantiene el proceso iterativo.

**`Ventajas`**
+ Alta disponibilidad (Mediante orquestación con Kubernetes | paralelización en servidores on-premise)
+ Costo
+ CI/CD
+ Escalabilidad (Puede ejecutarse en instancias de AWS, Azure o GCP)

**`Desventajas`**
+ Complejidad en el mantenimiento

**`Comandos de ejecución (Requiere variable de entorno para acceso a MongoDB Atlas)`**
```bash
docker build -t sismology .
```
```bash
docker run --name sismic-logger sismology
```

# Google Cloud Platform (GCP)
Mediante GCP se utiliza **Cloud Functions** para ejecutar directamente el código **cloud-function.py** que realiza todo el proceso de extracción, transformación y carga de los datos hacia MongoDB Atlas, especificando las dependencias necesarias en **requirements.txt** para finalmente asignarle el servicio **Cloud Scheduler** encargado de ejecutar la función descrita a intervalos de tiempo definidos.

**`Ventajas`**
+ Escalabilidad
+ Alta disponibilidad
+ Administración automática del servidor
+ Integración con otros servicios de Google Cloud
+ Monitoreo y registro

**`Desventajas`**
+ Costo

# Modelo Machine Learning

Para el modelo de Machine Learning se tomaron los datos de profundidad y magnitud, con el fin de encontrar un modelo de clasificacion automatico, es decir, un modelo de Machine Learning que entienda los datos y no uno que se encargue de predecir.

Antes de entrar en el modelo de Machine Learning, se evaluó la información proveniente de las APIs, donde se puede observar una gran cantidad de sismos para pequeños movimientos de poca profundidad y a medida que la profundidad del sismo aumenta, la cantidad de datos disminuye.

Para los modelos, se evaluaron 2 en este proyecto: K-Means y DBSCAN

K-Means

<a href="https://lh3.googleusercontent.com/drive-viewer/AFGJ81qdqnDmUQ49JfYEoQCUl_rBkJvzxPlbyFDXKN0NGv3eIcuRqY_YF8v3AqaePR__12adhVvJZJsU0K_gJw2M9beiMR6oSA=s1600?source=screenshot.guru"> <img src="https://lh3.googleusercontent.com/drive-viewer/AFGJ81qdqnDmUQ49JfYEoQCUl_rBkJvzxPlbyFDXKN0NGv3eIcuRqY_YF8v3AqaePR__12adhVvJZJsU0K_gJw2M9beiMR6oSA=s1600" /> </a>

Se utilizó el "método del codo" para obtener la cantidad óptima de clusters para nuestros servicios, que se calcula a través del error de cada cluster dando como resultado el numero 3.
De ahí, surge un problema, que el primer cluster contiene sismos que son imperceptibles para las personas, además de su gran cantidad, por lo que no serían un preocupación para nuestros usuarios y, una alerta constante de estos, podría llegar a provocar que lo usuarios se desmotiven en el uso de nuestro servicio y cuando uno de mayor fuerza aparezca, no seguir nuestras recomendaciones.
Es por eso que se decidió trabajar con 4 clusters y dejar por fuera el más débil, aunque sobrepase la cantidad de clusters óptimo, para nuestro servicio, consideramos que es mejor usar un 4to.
Quedando así, los clusters de 1 como el mas fuerte, 2 como el mas débil y 3 como el de nivel medio, dejando afuera el cluster numero 0.



DBSCAN

<a href="https://lh3.googleusercontent.com/drive-viewer/AFGJ81r1fkZThued3sQBA6Jch6vUXqJfIG__DFstLe4SME6qxgJnCBR8zRE9xzPbEfnAAb60fJzg9-nOG95AzzOa-QJXX2Yu=s2560?source=screenshot.guru"> <img src="https://lh3.googleusercontent.com/drive-viewer/AFGJ81r1fkZThued3sQBA6Jch6vUXqJfIG__DFstLe4SME6qxgJnCBR8zRE9xzPbEfnAAb60fJzg9-nOG95AzzOa-QJXX2Yu=s2560" /> </a>

Este modelo arrojó una cantidad óptima de 38 clusters, por lo que se decidió descartarlo debido a su gran cantidad y también lo poco unforme que estaban distribuidos los datos en los clusters.


# Colaboradores:

+ Juan Manuel Luna

+ Edgard Alfredo Huanca

+ David Alejandro Ortiz

+ Liznel Grimaldo

+ Cristian Zamudio