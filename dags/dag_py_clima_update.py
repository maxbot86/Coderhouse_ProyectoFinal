from __future__ import annotations

import sys, os
import logging
import time
from pprint import pprint
from datetime import datetime, timedelta
import pendulum
from climaAPI import climaMOD
from customDef import *
import airflow
from airflow.utils.dates import days_ago
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.python import (
    ExternalPythonOperator,
    PythonOperator,
)

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

default_args = {
    'owner': 'mmansilla',
    'start_date': days_ago(1),
    'email': ['mansilla.maximiliano@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}


date_to = (datetime.now() - timedelta(int(1))).strftime("%Y-%m-%d")
date_from = (datetime.now() - timedelta(int(2))).strftime("%Y-%m-%d")

def obtenerClima(ti):
    resultado, detalle = climaMOD.obtenerClima(date_from,date_to)
    ti.xcom_push(key='resultado_obtener', value=resultado)
    ti.xcom_push(key='resultado_detalle', value=detalle)


def notificarResultado(ti):
    resultado = ti.xcom_pull(key='resultado_obtener',task_ids = 'obtener_clima')
    detalle = ti.xcom_pull(key='resultado_detalle',task_ids = 'obtener_clima')

    if resultado:
        subject = 'Clima API - Obtenido OK'
        mensaje = "Se obtuvo correctamente el clima del dia de ayer"
    else:
        subject = 'Clima API - Obtenido ERROR'
        mensaje = "Ha ocurrido un error al obtener el clima de la API, se detalla a continuacion: \n"+detalle

    to_address = "mansilla.maximiliano@gmail.com"
    enviarCorreo(subject, mensaje, to_address)


with DAG(
    dag_id="dag_py_clima_update",
    default_args=default_args,
    schedule_interval='00 02 * * *',
    start_date=pendulum.datetime(2024, 1, 1, tz="America/Argentina/Buenos_Aires"),

    catchup=False,
    tags=["clima","python","daily", "obtener", "dev"],
):

    clima_etl = PythonOperator(task_id="obtener_clima", python_callable=obtenerClima)
    clima_notificar = PythonOperator(task_id="enviar_notificacion", python_callable=notificarResultado)
    clima_etl >> clima_notificar