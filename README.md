

## Materia: ST0263-251
Integrantes del proyecto:

* Delvin Rodríguez Jiménez - djrodriguj@eafit.edu.co
* Wendy Benítez Gómez - wdbenitezg@eafit.edu.co
* Fredy Cadavid Franco - fcadavidf@eafit.edu.co

Profesor

Edwin Nelson Montoya Múnera - emontoya@eafit.edu.co


# Analítica en Spark

# 1. Breve descripción de la actividad

## 1.1 **Aspectos desarrollados**
Durante la actividad, se realizaron los objetivos especificados en el proyecto para la ingesta, procesamiento y análisis de datasets, además de la configuración y automatización de la creación de clústeres de procesamiento de datos

 - [x] Analítica de datos con Spark y SparkSQL
 - [x] Creación y utilización de clústeres en AWS EMR para procesamiento
 - [x] Ingesta de datos a S3
 - [x] Automatización de los procesos de ETL
 - [x] Consulta y visualización de datos procesados

Para este proyecto, el equipo ha elegido el dataset de `OpenBank`, el cual posee diferentes indicadores mundiales y numerosas categorías en el desarrollo humano. El caso de estudio para este proyecto se ha concentrado en comparar el promedio de tasa de mortalidad de diferentes grupos (género, adultos, infantes) según cada país.
# 2. Arquitectura y diseño
Para esta actividad, se realizó el procesamiento de los datos utilizando una arquitectura basada en los servicios de AWS, como se muestra en la siguiente figura:
![Spark Data Processing in AWS](https://i.imgur.com/H0uhNG1.png)

Una máquina EC2 se encarga de realizar fetching del dataset de manera periódica a través de un cron job, realizando ingesta de los datos hacia S3 en una zona denominada `raw`, para su posterior procesamiento.

Se utilizó un servicio de procesamiento en clúster de AWS llamado EMR (Elastic Map Reduce), utilizando servicios como Hue, Hadoop. Se utiliza Spark para el procesamiento de los datos. 
Los datos son tratados y procesados, para finalmente ser llevados a las zonas `trusted`y después`refined` dependiendo del estado de dichos datos. Los datos estarán listos para ser luego consultados en AWS Athena, y visualizados mediante cualquier otra herramienta.

# 3. Ambiente de desarrollo

Para realizar el procesamiento de los datos, se utiliza `Apache Spark`, y se interactúa con este mediante `PySpark` en Python. Los requerimientos de qué librerías usar y sus versiones se encuentran en el archivo `requirements.txt` del proyecto.

### Preparación del entorno
`Spark` funciona en la JVM, por lo que será necesario instalar Java en la máquina de forma local. En Ubuntu, instalando OpenJDK en su versión 11 se haría de la siguiente manera:

    sudo apt-get  install  openjdk-11-jdk-headless

Después, se descarga Apache Spark. Para este proyecto se ha utilizado la versión 3.5.5:

    wget  -q  https://downloads.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz
    tar  xf  spark-3.5.5-bin-hadoop3.tgz
    
  Luego, se utiliza la librería `findspark` para localizar automáticamente la ruta donde se encuentra Spark:
  

    pip  install  -q  findspark

Se exportan las variables de entorno necesarias para localizar Spark y configurar el python de PySpark como `python3` en caso de que se ejecute de dicha forma

    export SPARK_HOME=/home/TU_USUARIO/spark-3.5.5-bin-hadoop3
    export PYSPARK_PYTHON=python3

Para ejecutar Spark se realiza lo siguiente con las variables de entorno ya exportadas:

    ${SPARK_HOME}/bin/pyspark

Ahora Spark estará ejecutándose.

Para enviar algún `job` hecho en Python via PySpark a Spark, se ejecuta lo siguiente:

    ${SPARK_HOME}/bin/spark-submit tu_archivo_job.py

Se puede revisar el estado de Spark entrando al servidor que corre localmente en `localhost:4040`

# 4. Ambiente de ejecución en producción
El ambiente de producción involucra la creación de buckets S3, un clúster de procesamiento EMR, y la creación de un `cron` (ya sea mediante AWS Lambda o en EC2) para la automatización de la ingesta y procesamiento de los datos. Además, se utiliza el servicio de AWS Athena para la consulta de los datos, y el uso de la librería `streamlit` de Python

 #### Almacenamiento S3
Inicialmente se deben crear dos buckets S3, el primero será donde vivirán los datos de ingesta y procesamiento, mientras que el segundo tendrá los `steps` que  se encuentran en este repositorio en el directorio `/scripts`.

![Buckets S3](https://i.imgur.com/uCL2LNm.png)

Dentro del bucket que contendrá los datos, se crean las siguientes 3 zonas:
![Data zones](https://i.imgur.com/MpCeXJg.png)
 
#### Clúster EMR
Las instrucciones en cómo se debe configurar el clúster para una etapa de pruebas inicial se encuentran en el siguiente [enlace](https://github.com/st0263eafit/st0263-251/blob/main/bigdata/00-lab-aws-emr/Install-AWS-EMR-7.9.0.pdf) en un documento PDF.

Para este despliegue, no es necesario que el usuario configure manualmente el clúster, ya que el `cron` creado para la ingesta de datos se encarga de esto automáticamente con las configuraciones previamente establecidas.

Lo que sí es necesario para monitorear el estado del clúster y los `steps o jobs` es abrir los puertos TCP `8888`, `8890`, `9870`, `14000` al grupo de seguridad del nodo maestro 

![Ports to be open](https://i.imgur.com/TSP3CXb.png)


#### Cron a través de EC2

Se crea una instancia EC2 de tamaño `t2.micro` con sistema operativo `Ubuntu`. A esta máquina se le debe establecer un rol de IAM que tenga acceso a S3 y además los permisos de creación de clúster EMR.

 Después de crearla, se actualizan los paquetes y se instala el paquete cron (en el caso en que no haga parte de la distro)

    sudo apt update
    sudo apt install cron -y

Se clona este repositorio en `/home/ubuntu` para tener los scripts que utilizará `cron`

    git clone https://github.com/DexterX12/Analitics-with-EMR.git

Luego de esto, se coloca el siguiente comando para configurar scripts:

    crontab -e

Se abrirá el archivo donde se encuentran los scripts línea por línea, se agrega el siguiente contenido:

    */20 * * * * /usr/bin/python3 /home/ubuntu/Analitics-with-EMR/cron_job.py >> /home/ubuntu/cron_logs.txt 2>&1

Esto especifica que cada 20 minutos se ejecutará el script de Python que obtiene los archivos del dataset, los sube a S3 y crea el cluster/le envía los steps; creará un log con el contenido de la ejecución de los scripts de Python.

#### Monitorización
Se puede ver el estado de cada job enviado por el cron previamente configurado.

Hay que dirigirse al servicio de Hue del nodo maestro, el cuál se puede encontrar en la pestaña de "Applications" en las propiedades del clúster (se debe de haber abierto el puerto 8888 previamente).

![Hue port](https://i.imgur.com/n8Cx1eF.png)

Al entrar pedirá usuario y contraseña, el usuario debe ser `hadoop`, la contraseña será la que guste.

Al entrar, hay que dirigirse a la sección de `Jobs`

![Sidebar](https://i.imgur.com/1Tj13VQ.png)


![Jobs](https://i.imgur.com/LPe48QM.png)

Esto muestra en tiempo real los `steps` o `jobs` que se están realizando/se han completado, además de proveer detalles para hacer troubleshoot de posibles errores.

#### Consulta con Athena

Las consultas a los archivos `Apache Parquet` se pueden realizar a través de Athena utilizando `SQL`. En este repositorio, dentro del directorio `athena/`, se encuentran los scripts de creación de base de datos y una consulta de prueba.

Es necesario configurar inicialmente el lugar donde las queries se guardarán:

![Athena Settings](https://i.imgur.com/Yl86s5j.png)

 ![Athena Query Example](https://i.imgur.com/ijGh3ze.png)

#### Consulta a través de API

Se creó un servicio de consulta de datos a través de API mediante API Gateway y la máquina EC2 que ya utiliza Streamlit para generar un reporte visual. Dentro del directorio `api/` se encuentra el servicio realizado en `flask`.

![API Routes](https://i.imgur.com/SrJ2pGQ.png)


Este posee las siguientes URL de consulta:

* [https://tijsdla2h9.execute-api.us-east-1.amazonaws.com/default/available-data-columns](https://tijsdla2h9.execute-api.us-east-1.amazonaws.com/default/available-data-columns): Obtiene los nombres de las columnas disponibles para realizar consulta de los datos.
* [https://tijsdla2h9.execute-api.us-east-1.amazonaws.com/default/all-countries-data-column?](https://tijsdla2h9.execute-api.us-east-1.amazonaws.com/default/all-countries-data-column?column=): Utilizando el parámetro de petición `column`, se especifica la columna de datos que se quiere consultar, obteniendo los datos de todos los países del dataset para esa columna en específico. La columna de datos a especificar se puede conocer mediante la ruta anterior.

#### Visualización con Streamlit
Además de poder consultar la información mediante Athena y por API, se utilizó `Streamlit`, una librería de Python que permite generar sitios web con ámbito en los datos.

Dentro de la misma máquina donde se ha clonado este repositorio para el `cron`, se encuentra el directorio `streamlit/` con el script para correr el servicio. Las librerías para poder ejecutarlo también estarán presentes en el `requirements.txt` del proyecto.

Estando en el directorio, se ejecuta un screen para que este se mantenga ejecutando todo el tiempo:

    screen -S nombre_de_la_screen

Después, se ejecuta el servicio:

    streamlit run main.py

Estará ejecutándose en la IP de la máquina en el puerto 8501

![Streamlit server](https://i.imgur.com/9IzpZiu.png)


![Visualización de los datos](https://i.imgur.com/aot4oIX.png)


En el dataset analizado, se puede observar como la tasa de mortalidad y esperanza de vida es muy variable por país/continente, y esto tiene que ver con la calidad de vida y acceso a los recursos de dichos países.



# Referencias
Islam, S. (2021, 12 de diciembre). Running Spark on Local Machine - Shariful Islam - Medium. _Medium_. https://medium.com/@sharifuli/running-spark-on-local-machine-c38957d022f4

CodeWithYu. (2023, 13 de noviembre). _Apache Spark Processing with AWS EMR | Data Engineering Project_ [Vídeo]. YouTube. https://www.youtube.com/watch?v=ZFns7fvBCH4

Saha, D. (2024, 16 diciembre). Amazon EMR: A Comprehensive Guide for Beginners - Dipan Saha - Medium. _Medium_. https://medium.com/@dipan.saha/amazon-emr-a-comprehensive-guide-for-beginners-a1d7a6014038
