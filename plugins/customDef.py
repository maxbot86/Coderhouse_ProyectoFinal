# Libreria Custom con funciones
# ===============================

from airflow.models import Variable

def sendFileSSH(host_server,port,username,password,localFile,remoteFile):
    import paramiko

    # Abrir el transporte
    transport = paramiko.Transport((host_server, port))
    transport.connect(username = username, password = password)

    sftp = paramiko.SFTPClient.from_transport(transport)
    
    try:
        print("Local: ")
        print(localFile)
        print("Remoto: ")
        print(remoteFile)
        sftp.put(localFile, remoteFile)
        print("Send File OK")
    except:
        print("ERROR al copiar el archivo")
 
    sftp.close()
    transport.close()


def enviarNotificacion(smtp_server, subject_email, body_email, mail_txt):
    import smtplib

    smtp_server =  Variable.get('smtp_server_custom')
    remitente = "Monitoreo <monitoreo@energia-argentina.com.ar>"
    destinatario = mail_txt
    asunto = subject_email
    mensaje = body_email

    email_struct = """From: %s 
To: %s 
MIME-Version: 1.0 
Content-type: text/html 
Subject: %s 

    %s
    """ % (
        remitente,
        destinatario,
        asunto,
        mensaje,
    )
    smtp = smtplib.SMTP(smtp_server)
    smtp.sendmail(remitente, destinatario, email_struct)


def rutaActual():
    import pathlib
    return pathlib.Path(__file__).parent.absolute()


def enviarCorreo(subject, mensaje,to_address ):
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    import smtplib
    # create message object instance 
    msg = MIMEMultipart()
    message = mensaje
    # setup the parameters of the message 
    password = Variable.get('smtp_server_from_passwd')
    msg['From'] = Variable.get('smtp_server_from_address')
    msg['To'] = to_address
    msg['Subject'] = subject
    # add in the message body 
    msg.attach(MIMEText(message, 'plain'))
    #create server 
    server = smtplib.SMTP(Variable.get('smtp_server_custom'),587)
    server.starttls()
    # Login Credentials for sending the mail 
    server.login(msg['From'], password)
    # send the message via the server. 
    server.sendmail(msg['From'], msg['To'], msg.as_string())
    server.quit()