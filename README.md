# Coderhouse-DE-Proyecto Final
#=============================
#=DEV: Maximiliano Mansilla
#= Linkedin: https://www.linkedin.com/in/maximiliano-gaston-mansilla-239444145/

Repositorio para Curso de Ingenieria de datos - Proyecto Final

Objetivos:

●Crear un pipeline que extraiga datos de una API pública de forma
constante combinándolos con información extraída de una base de
datos (mínimamente estas 2 fuentes de datos, pero pueden utilizarse
hasta 4).
● Colocar los datos extraídos en un Data Warehouse.
● Automatizar el proceso que extraerá, transformará y cargará datos
cuantitativos (ejemplo estos son: valores de acciones de la bolsa,
temperatura de ciudades seleccionadas, valor de una moneda
comparado con el dólar, casos de covid).
● Automatizar el proceso para lanzar alertas (2 máximo) por e-mail en
caso de que un valor sobrepase un límite configurado en el código.
Para finalizar el curso y poner en práctica todos los conocimientos
adquiridos te proponemos crear un pipeline que extraiga datos de
una API pública de forma constante combinándolos con información
extraída de una base de datos y colocándolos en un Data Warehouse.


Funcionamiento:

- Version de Python: 3.11.2
- Si ud posee el manual oficial de este repositorio, obviar los siguientes pasos.
- Para la inicializacion se requieren una serie de pasos:
	- ubicarse en la carpeta donde esta alojado el docker-compose.yaml
	- Ejecutar en la base de datos de destino el Script SQL adjunto ( 01-proyectoFinal.sql)
	- Ejecutar el comando "docker compose up airflow-init --build"
	- Ejecutar el comando "docker compose up"
	- Al finalizar la inicializacion del container,ingresar en la interfaz web y crear en la seccion "ADMIN->Variables" la siguientes variables:
	    "db_redshift_dbname": "NOMBRE DE LA DB",
		"db_redshift_hostname": "HOSTNAME DE LA DB",
		"db_redshift_passwd": "PASSWORD PARA CONEXION",
		"db_redshift_schema": "NOMBRE DEL ESQUEMA",
		"db_redshift_username": "USUARIO PARA LA CONEXION",
		"smtp_server_custom": "HOSTNAME DEL SMTP RELAY",
		"smtp_server_from_address": "EMAIL DESDE QUE SE ENVIARAN LAS NOTIFICACIONES",
		"smtp_server_from_passwd": "PASSWORD PARA CONEXION EN EL SMTP RELAY",
		"temperature_treshold_email": "EMAIL DE NOTIFICACION DE ALERTAS TEMPERATURA",
		"temperature_treshold_high": "LIMITE SUPERIOR DE TEMPERATURA A INFORMAR (INT)",
		"temperature_treshold_low": "LIMITE INFERIOR DE TEMPERATURA A INFORMAR (INT)"
	- -Finalizado este paso se puede recien ejecutar el DAG o activarlo para su ejecucion automatica.