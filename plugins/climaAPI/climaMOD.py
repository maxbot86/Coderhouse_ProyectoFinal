# ===============================
# =  DEV: Maximiliano Mansilla
# =  Proyecto: Data Engineering
# =
# =  Versionado:
# =      Version 01 - 2024-03-01
# =      Version 02 - 2024-03-05
# ===============================
from customDef import *
from dotenv import load_dotenv
import os
import logging
import sys
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from urllib.request import urlopen
import json
import pandas as pd
from airflow.models import Variable


def getWeatherHistory(coor, date_from, date_to):
    try:
        url_data = (
            "https://api.open-meteo.com/v1/forecast?latitude="
            + str(coor[0])
            + "&longitude="
            + str(coor[1])
            + "&hourly=temperature_2m,relative_humidity_2m,precipitation_probability,rain,wind_speed_10m&start_date="
            + str(date_from)
            + "&end_date="
            + str(date_to)
        )
        response = urlopen(url_data)
        data_json = json.loads(response.read())
        return {"result": True, "response": data_json}
    except Exception as e:
        return {"result": False, "response": e}


def WeatherJsonToDF(json_data):
    try:
        df = pd.DataFrame.from_dict(json_data["hourly"])
        return {"result": True, "response": df}
    except Exception as e:
        return {"result": False, "response": e}


def obtenerClima(date_from,date_to):
    # ======CUSTOMIZACION============
    path_main = str(rutaActual()) + "/"

    # == Archivo de Configuraion ==
    load_dotenv("/opt/airflow/plugins/climaAPI/config.env")

    # ============VARIABLES - VALORES DE CONFIG FILE ====================
    verbose_mode = bool(os.getenv("VERBOSE_MODE"))

    temperature_treshold_high = Variable.get('temperature_treshold_high')
    temperature_treshold_low = Variable.get('temperature_treshold_low')
    temperature_treshold_email = Variable.get('temperature_treshold_email')

    db_host = Variable.get('db_redshift_hostname')
    db_port = '5439'
    db_dbname = Variable.get('db_redshift_dbname')
    db_username = Variable.get('db_redshift_username')
    db_password = Variable.get('db_redshift_passwd')
    db_schema = Variable.get('db_redshift_schema')

    # ==============ARCHIVO LOG =============
    logFilename = path_main + str(os.getenv("LOG_FILE"))
    log_level = int(os.getenv("LOG_LEVEL"))
    print(logFilename)

    # ==LOGS - Functions
    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)
    handler = logging.FileHandler(logFilename)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.info("Se configuro el LOGGING")


    # ======================================================
    # ==ETL - EXTRACT======================================
    conn_string = f'redshift+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_dbname}'
    engine = create_engine(conn_string, echo=verbose_mode)
    connection = engine.connect()
    sql = "SELECT lat, lon FROM "+db_schema+".tbl_smecs GROUP BY lat, lon ORDER BY lat ASC"
    posiciones = pd.read_sql(sql,con=engine)
    for index, row in posiciones.iterrows():
        coor = [row['lat'].strip(),row['lon'].strip()]

        # =======================================
        logger.info("Getting Weather Data")
        logger.info("Coor. Latitude: " + coor[0])
        logger.info("Coor. Longitud: " + coor[1])
        logger.info("Date From: " + date_from)
        logger.info("Date To: " + date_to)

        # ==================================================

        dataGet = getWeatherHistory(coor, date_from, date_to)

        if dataGet["result"] == False:
            logger.error(dataGet["response"])
            return False, dataGet["response"]
        else:
            logger.info("Read Weather OK")
            resp = WeatherJsonToDF(dataGet["response"])
            if resp["result"] == False:
                logger.error(resp["response"])
                return False, resp["response"]
            else:
                logger.info("JSON->DF OK")
# ==================================================================
# ==ETL - TRANSFORM======================================
                df3 = resp["response"]
                # renombro la columna 'time' -> 'datetime_log' con un nombre mas representativo
                df3.rename(columns={"time": "datetime_log"}, inplace=True)
                # cambio el tipo de datos de la columna a 'datetime'
                df3["datetime_log"] = pd.to_datetime(df3["datetime_log"])
                # Agrego una columna TIMESTAMP para registrar el momento donde se agregaron los registros
                date_today = (datetime.now()).strftime("%Y-%m-%d %H:%M:%S")
                df3.insert(0, "datetime_reg", str(date_today))
                df3["datetime_reg"] = pd.to_datetime(df3["datetime_reg"])
                # agrego las dos columnas de cordenadas para luego poder utilizar el mismo proceso ETL para recolectar datos de otra ubicacion.
                df3.insert(1, "coor_lat", coor[0])
                df3.insert(2, "coor_lon", coor[1])
                
                #== TRESHOLD CHECK ====================
                for index, registro in df3.iterrows():
                    if float(registro['temperature_2m'])>float(temperature_treshold_high) or float(registro['temperature_2m'])<float(temperature_treshold_low):                        
                        subject = "ALERTA - Temperatura fuera de rango"
                        mensaje = f"Se detecto una temperatura fuera de los rangos establecidos > a {temperature_treshold_high} C° o < a {temperature_treshold_low} C° \n"
                        mensaje += "Fecha: "+str(registro['datetime_log'])+" \n"
                        mensaje += "Temperatura: "+str(registro['temperature_2m'])+" C°  \n"
                        mensaje += "Coordenadas: "+str(registro['coor_lat'])+" / "+str(registro['coor_lon'])+"  \n"
                        enviarCorreo(subject, mensaje, temperature_treshold_email)
                # ==================================================================
                # ==ETL - LOAD======================================
                try:
                    conn_string = f'redshift+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_dbname}'
                    engine = create_engine(conn_string, echo=verbose_mode)
                    connection = engine.connect()
                                      
                    logger.info("Connection - OK")
                        
                    res = df3.to_sql(
                        name="logs_weather",
                        schema=db_schema,
                        con=connection,
                        if_exists="append",
                        index=False,
                    )
                    logger.info("Reading Reg: " + str(res))
                    logger.info("Add Rows - OK")
                except Exception as e:
                    logger.error(e)
                    return False, e
                logger.info("END")
    return True, "OK"