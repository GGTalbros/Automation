import pyodbc
import pandas as pd
import os.path
from datetime import datetime
import shutil
import csv    
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

cur = cursor.execute('''select code from tal_dly_mail''')
code_fetch = cur.fetchall()
#print(code_fetch)

for codes in code_fetch :
    print('\n')
    print('codes',codes)
    cur = cursor.execute('''select Qstring from OUQR,oalt where OUQR.IntrnalKey =oalt.QueryId and oalt.Active = 'Y' and code = ? ''' , codes)   
    Qsring_fetch = cur.fetchall()
    Qstring = Qsring_fetch[0][0]
    #print(Qstring)
    

    emails = cursor.execute('''with test_cte (emailids) as (select ousr.E_Mail from oalt,ALT1,OUSR where oalt.Code=ALT1.Code 
                            and ALT1.UserSign= OUSR.USERID and ALT1.code = ? and E_mail is not null) SELECT Stuff(
                            (
                            SELECT ', ' + emailids
                            FROM test_cte
                            FOR XML PATH('')
                            ), 1, 2, '') AS emailids''',codes)
     
    all_code = emails.fetchone()
    r_mail = all_code.emailids
    #print(r_mail)
    data = cursor.execute(Qstring)
    all_query = data.fetchall()
    #print('all_query.rowcount',all_query.rowcount)
    #print(all_query)
    #print('Done')
    
    #print('data',data)
    print('data.rowcount',data.rowcount)
    if data.rowcount == 0 :
        #print('skipped')
        pass
        
    else :
        cur = cursor.execute('''select header from tal_dly_mail where code = ?''', codes)
        header_fetch = cur.fetchone()
        header = header_fetch[0]
        #print(header)
        
        file = open(r"C:\Users\abc\Supp_mail.csv", 'w', newline='') 
        writer = csv.writer(file)
        
        if header is None :
            pass       
        else :
            pass
            #print('Good',codes)
            header_list = list(header.split(","))
            
            writer.writerow(header_list)
            #print(header_list)

        writer.writerows(all_query)
        file.close()
       
        
        fromaddr = 'it@bnt-talbros.com'
        password = '9555812686'
        toaddr = 'support@talbrosaxles.com'
        #'arpitamohanty916@gmail.com'
        
        
        msg = MIMEMultipart()
        
        msg['From'] = fromaddr
        msg['To'] = toaddr
        #  msg['Cc'] = cc
        msg['Subject'] = "SAP Business One mail message, the subject is in the message body."
        
        cur = cursor.execute('''select name from tal_dly_mail where code = ? ''' , codes)
        name_fetch = cur.fetchone()
        name = name_fetch[0]
        print(name)


        cur = cursor.execute('''select name from OUQR,oalt where OUQR.IntrnalKey =oalt.QueryId and oalt.Active = 'Y' and code = ? ''' , codes)
        body_fetch = cur.fetchone()
        body = body_fetch[0]
        print(body)
        
        #body = name
        
        msg.attach(MIMEText(body, 'plain'))
        
        filename = r"C:\Users\abc\Supp_mail.csv"
        attachment = open(filename, "rb")
        
        p = MIMEBase('application', 'octet-stream')
        
        p.set_payload((attachment).read())
        
        encoders.encode_base64(p)
        
        file_name = name+ ".csv"
        p.add_header('Content-Disposition', 'attachment; filename= "%s"' %file_name)
        
        msg.attach(p)
        
        server = smtplib.SMTP('smtp.gmail.com', 587)
        
        server.starttls()
        
        server.login(fromaddr,password)
        
        text = msg.as_string()
        
        server.send_message(msg)
        
        server.quit()
    
os.remove(r"C:\Users\abc\Supp_mail.csv")
    
 
conn.close()