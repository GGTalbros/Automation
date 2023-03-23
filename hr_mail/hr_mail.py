import pyodbc
import pandas as pd
from datetime import datetime
import os
import logging
from logging.handlers import RotatingFileHandler
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from os.path import exists as file_exists
from dateutil import parser

conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};"
                   "Server=192.168.0.60;"
                   "PORT=1433;"
                   "Database=Talbros_Live;"
                   "UID=sa;"
                   "PWD=tel@2017;")

cursor = conn.cursor()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(filename=r'.\logfile.log', maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)

      


def email(*val):
    try:
            sender_email =  'it@bnt-talbros.com'
            rec_email = val[0]
            password = '9555812686'
            
            msg = MIMEMultipart()                   
            msg['From'] = sender_email
            msg['To'] = rec_email
            msg['Subject'] = val[2]
            body =val[3]
            msg.attach(MIMEText(body, 'plain'))
            filename = val[1]
            attachment =  open("//192.168.0.50/hr/" + filename , "rb")
            p = MIMEBase('application', 'octet-stream')
            p.set_payload((attachment).read())
            encoders.encode_base64(p)
            p.add_header('Content-Disposition', "attachment; filename=%s"%filename )
            msg.attach(p)
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(sender_email,password)
            text = msg.as_string()
            server.send_message(msg)
            server.quit()
            
            logger.info('Mail sent')

    except Exception as e:
        logger.exception("Email Sending Failed..{}".format(e))
        raise Exception("Email Sending Failed..{}".format(e))
        

cur_day = datetime.today().strftime('%A')

cur = cursor.execute("select group_type from tal_hr_mail")
group_type_data = cur.fetchall()

for row in group_type_data :

    cur = cursor.execute('''select email_ids, file_name, Body, Subject, start_time, end_time, day , (SELECT cast(CONVERT (TIME, CURRENT_TIMESTAMP) as time)) as 'Current',
    CASE
        WHEN (SELECT cast(CONVERT (TIME, CURRENT_TIMESTAMP) as time)) >= start_time and (SELECT cast(CONVERT (TIME, CURRENT_TIMESTAMP) as time)) < end_time  THEN 'Y'
        ELSE 'N'
    END as result
    from tal_hr_mail where group_type = ? ''', row)
    hr_data = cur.fetchall()
    
    email_ids = hr_data[0][0]
    file_name = hr_data[0][1]
    Body = hr_data[0][2]
    Subject = hr_data[0][3]
    start_time = hr_data[0][4]
    end_time = hr_data[0][5]
    
    day_fetch = hr_data[0][6]
    day = list(day_fetch.split(","))
    
    cur_time = hr_data[0][7]
    result = hr_data[0][8]
    
    if result == 'Y' :
        
        backup_storage_available = os.path.isdir(r"\\192.168.0.50\hr")
        
        if backup_storage_available:
            logger.info("Backup storage already connected.")
        else:
            logger.info("Connecting to backup storage.")
        
        mount_command = "net use /user:" +"Administrator"+ " " +r"\\192.168.0.50\hr" + " " + "Limited#@555" 
        os.system(mount_command)
        backup_storage_available = os.path.isdir(r"\\192.168.0.50\hr")
        
        if backup_storage_available:
            logger.info("Connection success.")
        else:
            raise Exception("Failed to find storage directory.")

        if day == 'Everyday' :
            
            email(email_ids,file_name,Subject,Body)
            
        elif any(cur_day in s for s in day) :
           
            email(email_ids,file_name,Subject,Body)
        
        else :
            
            email(email_ids,file_name,Subject,Body)
        
    else  :
        logger.info('Current time does not Match')
