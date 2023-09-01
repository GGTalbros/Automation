import pyodbc
import pandas as pd
import os.path
from datetime import datetime
import shutil
import csv    
import copy
from os.path import exists as file_exists
from dateutil import parser
import math
import logging
from logging.handlers import RotatingFileHandler
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import configparser
import sys                                                                        
import getopt
import os
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(filename=r'.\supp_logfile.log', maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

logger.info('\n')

# Convert file data in binary data
def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData

# function to update binary data (overall file) in table
def updateBLOB( gr_file):
    
    try:        
        cursor = conn.cursor() 
        sql_insert_blob_query = """ Update supp_adt_trl set data = ? where datecreated = (select max(datecreated) from supp_adt_trl)"""

        file = convertToBinaryData(gr_file)     
        # Convert data into tuple format
        insert_blob_tuple = ( file)  
        result = cursor.execute(sql_insert_blob_query, insert_blob_tuple)
        conn.commit()
        logger.info("Blob Updated")
    except Exception as e:
        conn.rollback()
        logger.exception("Failed updating BLOB data into SQL table {}".format(e))
        raise Exception("Failed updating BLOB data into SQL table {}".format(e))

#inserting cardcode,path,status
def ins_adt_trl(*arg) :  
    try:        
        cursor = conn.cursor() 
        cardcode = arg[0]
        module = arg[1]
        path = arg[2]
        status = arg[3]
        txt_data = arg[4]
        cursor.execute("Insert into supp_adt_trl values(?,?,?,cast('a' as varbinary(max)),getdate(),?,NULL,?)",cardcode,module,path,status,txt_data)
        conn.commit()
        logger.info("Audit Trail Inserted for module {}".format(module))
    except Exception as e:
        conn.rollback()
        logger.exception("Failed inserting into Audit Trail {}".format(e))
        raise Exception("Failed inserting into Audit Trail {}".format(e))
    
#updating path,status    
def status_upd_adt_trl(*arg) :
    try:
        cursor = conn.cursor()
        if len(arg)==1 :
            error_msg = arg[0]
            logger.info(error_msg)
            cursor.execute("Update supp_adt_trl set error_msg = ?, status = 'Error' where datecreated = (select max(datecreated) from supp_adt_trl)", error_msg)
        else :
            path = arg[0]
            status = arg[1]
            cursor.execute("Update supp_adt_trl set path = ?, status = ? where datecreated = (select max(datecreated) from supp_adt_trl)", path,status)
        
        conn.commit()
        logger.info("Audit Trail Updated!!")
    except Exception as e:
        conn.rollback()
        logger.exception("Updation of  Audit Trail Failed..{}".format(e))
        raise Exception("Updation of  Audit Trail Failed..{}".format(e))
        
        
# Email function with attachment or without attachment
def email(*val):
    try:
        sender_email = parser.get(arg,'sender')
        rec_email = parser.get(arg,'receiver')
        password = parser.get(arg,'password')
        
          
        if len(val)== 4:
         # Send Email with error file
            msg = MIMEMultipart()                   
            msg['From'] = sender_email
            msg['To'] = rec_email
            msg['Subject'] = val[2]
            body = val[3]
            msg.attach(MIMEText(body, 'plain'))
            filename = val[0]
            attachment = open(filename, "rb")
            p = MIMEBase('application', 'octet-stream')
            p.set_payload((attachment).read())
            encoders.encode_base64(p)
            p.add_header('Content-Disposition', "attachment; filename=%s"%val[1] )
            msg.attach(p)
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(sender_email,password)
            text = msg.as_string()
            server.send_message(msg)
            server.quit()
            
            logger.info('Mail sent')
            
        else:
           msg = MIMEMultipart()

           msg['From'] = sender_email
           msg['To'] = rec_email
           msg['Subject'] = val[0]
           body = val[1]
           msg.attach(MIMEText(body, 'plain'))
           server = smtplib.SMTP('smtp.gmail.com', 587)
           server.starttls()
           server.login(sender_email,password)
           text = msg.as_string()
           server.send_message(msg)
           server.quit()
    except Exception as e:
        logger.exception("Email Sending Failed..{}".format(e))
        raise Exception("Email Sending Failed..{}".format(e))
        
def format_db_err_message(message) :
    return message[:200] if len(message) > 200 else message

#raise Exception and Log Exception
def raise_and_log_error(err_msg,module):
    logger.info(error_msg)
    conn.rollback()
    email("Error in {a}".format(a=module),error_msg)
    status_upd_adt_trl(format_db_err_message(error_msg))

# function for part list    
def part_list_func(list_count):
    try:
        cursor = conn.cursor()
        if(list_count == 4 ): # when fourth field given
                        
                        input_data = list_input[3]
                        
                        if input_data.startswith("~"): # element not in fourth field
                            logger.info("Not Part List is  :: {a}".format(a= list_input[3]))
                            input_data = input_data.translate({ord('~'): None})
                            not_part_list = input_data.split(";")
                            not_part_list_arg = copy.deepcopy(not_part_list)
                            not_part_list_arg.append(card_code)
                            not_part_list_arg.append(sdate)
                            not_part_list_arg.append(edate)
                            plc = '?'
                            plc = ','.join(plc for unused in not_part_list)
                            query = """SELECT distinct inv1.SubCatNum from INV1 inv1 inner join 
                                    OINV on oinv.DocEntry = inv1.DocEntry where inv1.SubCatNum not in(%s) and OINV.CardCode = ? 
                                    and inv1.ItemCode != 'AXLE' and inv1.TargetType NOT IN ('14')
                                    and OINV.DocDate >= ? and OINV.DocDate < ? and OINV.U_SuppGR is not NULL and oinv.CANCELED = 'N'""" %plc
                            cur = cursor.execute(query,not_part_list_arg)
                            all_row = cur.fetchall()
                            part_list = [''.join(i) for i in all_row]
                            logger.info("Parts for which {b} has to be prepared are :{a}".format(a=part_list,b=key[j]))
                
                        else : # for given element in fourth field
                            part_list = list_input[3].split(";") 
                            logger.info("Part List for {b} is {a}".format(a=part_list,b=key[j]))

                            
        if(list_count == 3 ): # fourth field not given
            cur = cursor.execute("""SELECT distinct inv1.SubCatNum from INV1 inv1 inner join 
                                    OINV on oinv.DocEntry = inv1.DocEntry where OINV.CardCode = ? and inv1.ItemCode != 'AXLE' 
                                    and inv1.TargetType NOT IN ('14') and OINV.U_SuppGR is not NULL and  OINV.DocDate >= ? and OINV.DocDate < ? and oinv.CANCELED = 'N'""",card_code,sdate, edate)
            all_row = cur.fetchall()
            part_list = [''.join(i) for i in all_row]
            logger.info("Part List for {b} is {a}".format(a=part_list,b=key[j]))
            
        return(part_list) 
        
    except Exception as e:
        logger.exception(" Failed to get part list {}".format(e))
        raise Exception(" Failed to get part list {}".format(e))
        
def file_exists(directory, start,customer):
    files = os.listdir(directory)
    matching_files = [file for file in files if file.startswith(start)]
    if len(matching_files) == 1:
        GrPath = os.path.join(directory, matching_files[0])
        return GrPath
    elif len(matching_files) == 0 :
        pass
    else :
        logger.info("More than one GRN File present in folder for {a}".format(a=customer))

parser = configparser.ConfigParser()
parser.read(r'.\supp_automation_config.properties')
argv = sys.argv [ 1: ]  
opts , args = getopt.getopt ( argv, "e:" ) 

# argument for environment
for opt , arg in opts:
    if opt in ['-e']:
        if arg in [ 'LOCAL' ] or arg in [ 'Local' ] or arg in [ 'local' ] :
            arg = 'LOCAL'
            local = parser.get(arg, 'supplementary.basepath')
            basepath = local
            logger.info('Environment is :: {a}'.format(a=arg))
    
        
        elif arg in [ 'PROD' ] or arg in [ 'Prod' ] or arg in [ 'prod' ] : 
            arg = 'PROD'
            prod = parser.get(arg, 'supplementary.basepath')
            basepath = prod
            logger.info('Environment is :: {a}'.format(a=arg))
            
        elif arg in [ 'UAT' ] or arg in [ 'Uat' ] or arg in [ 'uat' ] : 
            arg = 'UAT'
            uat = parser.get(arg, 'supplementary.basepath')
            basepath = uat
            logger.info('Environment is :: {a}'.format(a=arg))
            
#if no argument is given exception is thrown
if len(argv) == 0 :
    logger.exception('No Environment is selected')
    raise Exception('no environment argument is given')
      
#dictionary values from properties file
dictnry = eval(parser.get(arg,'Supp.folder.dict'))
logger.info('Dictionary is :: {a}'.format(a=dictnry))


# Connection with database  ( local or production)
conn = pyodbc.connect(Driver="%s" %parser.get(arg, 'DB.Driver'),
        Server="%s" %parser.get(arg, 'DB.Server'),
        PORT="%s" %parser.get(arg, 'DB.PORT'),
        Database = "%s" %parser.get(arg, 'DB.Database'),
        UID="%s" %parser.get(arg, 'DB.UID'),
        PWD="%s" %parser.get(arg, 'DB.PWD'))


for i in dictnry: # loop on key of dictionary( customers )
    logger.info('Customer is :: {a}'.format(a=i))
    key = dictnry[i].split(",")
    
    for j in range(len(key)): # loop on the length of  particular key values ( GRN/Price/....)
        logger.info('File is :: {a}'.format(a=key[j]))
        path_key = f'{i}{"."}{key[j]}{".Filename"}'
        cardcode_key = f'{i}{"."}{"Cardcode"}'
               
        cardcode = parser.get(arg,cardcode_key)
        
        path = os.path.join(basepath,i,key[j],parser.get(arg,path_key)) # original file path       
        if key[j] == 'Grn' :
            filename_key = f'{i}{"."}{key[j]}{".Filestartname"}'
            
            start_filename = parser.get(arg, filename_key)
            GrPath = file_exists(path,start_filename,i)
            if GrPath != None :
                try:
                    cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
                    
                    split_path_name = os.path.splitext(GrPath)[0]
                    new_path_name = f'{split_path_name}{" "}{cur_ts}{".xls"}'
                    os.rename(GrPath, new_path_name)
                    
                    logger.info('File Location is :: {a}'.format(a=new_path_name))
                    ins_adt_trl(cardcode,key[j],new_path_name,'START',None)
                    updateBLOB(new_path_name)                         
                    
                    copy = f'{i}{"."}{"Grn"}{".Filename.copy"}'
                    path_copy = os.path.join(basepath,i,key[j],parser.get(arg,copy)) # copy the file from original path to another path 
                    split_path_copy = os.path.splitext(path_copy)[0]
                    new_path_copy = f'{split_path_copy}{" "}{cur_ts}{".xls"}'
                    
                    renam = f'{i}{"."}{"Grn"}{".Filename.renam"}'
                    path_renam = os.path.join(basepath,i,key[j],parser.get(arg,renam)) # rename the copied file with csv extension
                    split_path_renam = os.path.splitext(path_renam)[0]
                    new_path_renam = f'{split_path_renam}{" "}{cur_ts}{".csv"}'

                    mov = f'{i}{"."}{"Grn"}{".Filename.mov"}'
                    path_mov = os.path.join(basepath,i,key[j],parser.get(arg,mov))  # move the original  path file to processed path
    
                    logger.info('GRN File is found')                       
                    #copying file
                    shutil.copy(new_path_name, new_path_copy)                        
                    #changing format from xls to csv
                    os.rename(new_path_copy, new_path_renam)
                    status_upd_adt_trl(new_path_copy,'PROCESSING')
                    #reading xlsx
                    df = pd.read_excel(new_path_renam) 
                    #renaming the column name 
                    df.rename( columns=({ 'GR No' : 'GR_no', 'GR Date' : 'GR_dt', 'ASN No' : 'WSN_ASN', 'GR Qty' : 'GR_qty', 'Ref Document No': 'DC_no'}), inplace=True,)                   
                    df.head()                    
                    #converting dateformat
                    df['GR_dt'] = pd.to_datetime(df['GR_dt'],infer_datetime_format=True)                  
                    #storing date after conversion
                    df.to_csv(new_path_renam)  
                    if 'Rej_qty' not in df.columns:
                        df['Rej_qty'] = 0
                    #inserting values in temp table
                    cursor = conn.cursor()
                
                    for row in df.itertuples():
                        cursor.execute('''
                                INSERT INTO supp_int_data(GR_dt, GR_no, DC_no, Rej_qty, GR_qty, WSN_ASN,Datecreated)
                                VALUES (?,?,?,?,?,?,getdate())
                                ''',
                                row.GR_dt,
                                row.GR_no,
                                row.DC_no,
                                row.Rej_qty,
                                row.GR_qty,
                                row.WSN_ASN
                                )
                
                    #updating main table
                    cursor.execute(""" UPDATE m SET m.U_SuppGRDate = t.[GR_dt], m.U_SuppDc = t.[DC_no], m.U_SuppGR = t.[GR_no], 
                                        m.U_SuppRejQty = t.[Rej_qty], m.U_SuppGRQty = t.[GR_qty], m.U_SuppWSNASN = t.[WSN_ASN],
                                        m.U_SuppDateCreated = t.[Datecreated] FROM OINV AS m INNER JOIN supp_int_data AS t 
                                        ON m.docnum = t.[DC_no] and m.[CardCode]='%s'"""%cardcode)

                    #deleting the values of temp table 
                    cursor.execute('Truncate table supp_int_data')
                    
                    #commiting changes in SQL
                    conn.commit()
                    logger.info('Values saved in DB')                       
                    #removing file 
                    os.remove(new_path_renam)                     
                    #moving the main file
                    cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
                    shutil.move(new_path_name, path_mov + cur_ts + ".xls")
                    status_upd_adt_trl(path_mov + cur_ts + ".xls",'PROCESSED')
                    #sending mail
                    email("GRN DATA","GRN Data Updated")
                    logger.info('GRN Updated')

                except Exception as e:                    
                    logger.exception(e)
                    conn.rollback()
                    message = "%s"%e
                    email("Error in GRN",traceback.format_exc())
                    status_upd_adt_trl(format_db_err_message(traceback.format_exc()))
                    
                logger.info("GRN File WORK DONE")
                
            else :
                logger.info("GRN File for {a} is not found".format(a=i))
            
        if key[j] == 'Price' :
            if os.path.isfile(path):
                chk_err    = 0                
                cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
                split_path_name = os.path.splitext(path)[0]
                new_path_name = f'{split_path_name}{" "}{cur_ts}{".txt"}'
                os.rename(path, new_path_name)
                
                logger.info('File Location is :: {a}'.format(a=new_path_name))
                try:
                    
                    cursor = conn.cursor()
                    
                    # reading value fromm text file 
                    file_prc_txt = open(new_path_name,"r")
                    txt_input = file_prc_txt.readlines()
                    txt_input_conv = '%s'%txt_input
                    ins_adt_trl(cardcode,key[j],new_path_name,'START',txt_input_conv) #inserting price file details in SQL
                    list_input = txt_input[0].split(",")
                    logger.info("Input Data is  :: {a}".format(a=list_input))
                    list_count = len(list_input)
                    
                    card_code  = list_input[0]
                    sdate      = list_input[1]
                    edate      = list_input[2]
                    
                    header_err_lst = ["CardCode", "New Price from System", "New Price from Input Data"]
                    
                    part_list= part_list_func(list_count) # calling part list function
                    
                    cur = cursor.execute("select top 1 cast(supp_date as date) from supp_txns where status = 'A' order by supp_date desc")
                    supptbl_date = cur.fetchone()
                    
                    cur = cursor.execute("select top 1 cast(supp_date as date) from supp_prc_txns where status = 'A' order by supp_date desc")
                    prctbl_date = cur.fetchone()
                    
                    none_flag = 'I'
                    equal_flag = 'I'
                    
                    if supptbl_date is None and prctbl_date is None :
                        none_flag = 'A'
                    
                    elif supptbl_date is None or prctbl_date is None :
                        logger.info('Complete the process')
                    
                    else :
                        if (supptbl_date[0] == prctbl_date[0]) :
                            equal_flag = 'A'                   
                                        
                    if  none_flag == 'A' or  equal_flag == 'A' :
                    
                        for part in part_list:
                            
                            part_no = part

                            logger.info(part_no)
                            
                            cur = cursor.execute("""select rdr1.price from ordr inner join rdr1 on 
                                                ordr.DocEntry = rdr1.DocEntry  where CardCode = ? and SubCatNum = ? 
                                                and ordr.U_Suppdate = ? and ordr.series = '846' and CANCELED = 'N'
                                                and ordr.docdate = (select max(ordr.docdate) from ordr inner join rdr1 on 
                                                ordr.DocEntry = rdr1.DocEntry  where CardCode = ? and SubCatNum = ? 
                                                and ordr.U_Suppdate = ? and ordr.series = '846' and CANCELED = 'N')""", card_code,part_no,sdate,card_code,part_no,sdate)
                            
                            #Check null of new_prc_cur
                            new_prc_cur = cur.fetchall()
                            #new_prc_list = [''.join(i) for i in new_prc_cur]
                            if len(new_prc_cur) == 0:
                                error_msg = "Failed to get a record from DB for New Price.Card Code is :: {a}. Part No is :: {b}. Start Date is :: {c}".format(a=card_code,b=part_no,c=sdate)
                                raise_and_log_error(error_msg,'Price')
                                chk_err    = 1
                                break                            
                            
                            #Duplicate PO raised
                            elif len(new_prc_cur) > 1:
                                error_msg = "Duplicate PO found in DB.Card Code is :: {a}. Part No is :: {b}. Start Date is :: {c}".format(a=card_code,b=part_no,c=sdate)
                                raise_and_log_error(error_msg,'Price') 
                                chk_err    = 1
                                break
                                
                            else :
                                new_prc_db =  new_prc_cur[0][0] 
                                
                            #Check if for a part there are more than 1 invoice price present for date range if yes then
                            #error please ask user to prepare supplementary or get 2 sets of Prices for that part from OEM.
                            cur = cursor.execute("""select distinct inv1.price from INV1 inv1 inner join 
                                                OINV on oinv.DocEntry = inv1.DocEntry where  OINV.U_SuppGR is not NULL and OINV.CardCode = ? and inv1.ItemCode != 'AXLE' 
                                                and inv1.TargetType NOT IN ('14') and OINV.DocDate >= ? and OINV.DocDate < ? and oinv.CANCELED = 'N' 
                                                and inv1.subcatnum = ?""",card_code,sdate,edate,part_no)
                            
                            all_row = cur.fetchall()
                            price_part_list = [''.join(str(i)) for i in all_row]
                            logger.info(price_part_list)
                            if len(price_part_list) > 1 :
                                error_msg = "Please resolve multiple price issue for the part. Card Code is :: {a}. Part No is :: {b}. Start Date is :: {c}. End Date is {d}. For this combination, there are multiple price from invoice : {e}".format(a=card_code,b=part_no,c=sdate,d=edate,e=price_part_list)
                                raise_and_log_error(error_msg,'Price')
                                chk_err    = 1
                                break
                            #Get Old Price based on invoice or supp_txn table
                            cur = cursor.execute("""with inv_cte(inv_nos,inv_dt,inv_prc) as(
                                                    SELECT top 1 oinv.docnum,oinv.docdate,inv1.price from INV1 inv1 inner join 
                                                    OINV on oinv.DocEntry = inv1.DocEntry where OINV.CardCode = ? and inv1.ItemCode != 'AXLE' 
                                                    and inv1.TargetType NOT IN ('14') and OINV.U_SuppGR is not NULL and OINV.DocDate >= ? and OINV.DocDate < ?  and oinv.CANCELED = 'N' and inv1.subcatnum = ? order by DocDate asc)
                                                    select COALESCE (new_prc,inv_prc)old_inv_prc from inv_cte LEFT OUTER JOIN supp_txns 
                                                    on inv_cte.inv_nos = supp_txns.org_inv_no and supp_txns.status = 'A' and supp_date >= (select max(supp_date) from supp_txns where supp_date < ? and status = 'A' and party_partno  = ?)""", card_code,sdate,edate,part_no,sdate,part_no)
                            #GG : check multiple occurence                        
                            old_prc_cur = cur.fetchall()
                            old_price_list = [''.join(str(i)) for i in old_prc_cur]
                            if len(old_price_list) == 0: 
                                error_msg = "Failed to get a record from DB for OLD Price. Card Code is :: {a}. Part No is :: {b}. Start Date is :: {c}. End Date is {d}".format(a=card_code,b=part_no,c=sdate,d=edate)
                                raise_and_log_error(error_msg,'Price')
                                chk_err    = 1
                                break
                                
                                
                                
                            else :
                                formatted_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                old_prc_db  =   old_prc_cur[0][0]

                            
                                cursor.execute('''
                                                INSERT INTO supp_prc_txns (cardcode, party_partno, old_inv_prc, new_po_prc,supp_date,supp_date_end,datecreated,status)
                                                VALUES (?,?,?,?,?,?,?,?)
                                                ''',
                                                card_code,
                                                part_no,
                                                old_prc_db,
                                                new_prc_db,
                                                sdate,
                                                edate,
                                                formatted_date,
                                                'A'
                                            )
    
                            logger.info(part_no)
                        if(chk_err == 0):
                            conn.commit()
                            status_upd_adt_trl(new_path_name,'PROCESSED')
                            logger.info('Price Data Updated')
                            file_prc_txt.close()
                            os.remove(new_path_name)
                    
                    else :
                        logger.info('Supplementary is not generated')
                        error_msg = "Supplementary is not generated.Please make Supplementary first for previous date "
                        raise_and_log_error(error_msg,'Price')
                    
                except Exception as e:
                    logger.exception(e)
                    conn.rollback()
                    message = "%s"%e
                    email("Error in Price",traceback.format_exc())
                    status_upd_adt_trl(format_db_err_message(traceback.format_exc()))  
                logger.info('Price File Work Done')
            
            else :
                logger.info(" Text File for Price in {a} is not found".format(a=i)) 
                
            
        if key[j] == 'Dtw' :
            if os.path.isfile(path):
                
                cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
                split_path_name = os.path.splitext(path)[0]
                new_path_name = f'{split_path_name}{" "}{cur_ts}{".txt"}'
                os.rename(path, new_path_name)
                
                file_name = f"{'DTW'}{' '}{cur_ts}{'.csv'}"
                cursor = conn.cursor()
                
                file_dtw_txt = open(new_path_name,"r")
                logger.info('File Location is :: {a}'.format(a=new_path_name))
                try: 
                    
                    dtw_txt_input = file_dtw_txt.readlines()
                    dtw_txt_input_conv = '%s'%dtw_txt_input
                    ins_adt_trl(cardcode,key[j],new_path_name,'START',dtw_txt_input_conv) #inserting DTW file details in SQL
                    
                    list_input = dtw_txt_input[0].split(",")
                    logger.info("Input Data in DTW is  :: {a}".format(a=list_input))
                    list_count = len(list_input)
                    
                    #GG : Write in proper Location with withstamp
                    #cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
                    #file_name = f"{'DTW'}{' '}{cur_ts}{'.csv'}"
                    file = open(file_name, 'w', newline='') 
                    writer = csv.writer(file)
                    fieldnames = list(parser.get(arg,"Dtw.Header").split("|"))
                    writer.writerow(fieldnames)  
                    file.close()
                   
                    
                    card_code  = list_input[0]
                    sdate      = list_input[1]
                    edate      = list_input[2]
                    chk_err    = 0
                    
                    part_list= part_list_func(list_count)
                    #logger.info("Part List in DTW generation is {a}".format(a=part_list))

                    cur = cursor.execute("select count(distinct u_suppdate) from oinv where u_suppdate = ? and CANCELED = 'N'", sdate)
                    date_count = cur.fetchone()
                    
                    if date_count[0] == 0 :
                    
                        for part in part_list:
                            
                            part_no = part

                            logger.info(part_no)
                            
                            cur = cursor.execute('''WITH details(disdate,qty,tel_partno,party_partno,partycode,invoice,whs,taxcode,docdate) AS (
                                                    SELECT CONVERT(varchar,CAST(oinv.DocDate as date)) disdate, inv1.Quantity,inv1.ItemCode,INV1.SubCatNum,
                                                    OINV.CardCode, CONVERT(varchar,OINV.DocNum), inv1.WhsCode,inv1.TaxCode,oinv.DocDate from INV1 inv1
                                                    inner join OINV on oinv.DocEntry = inv1.DocEntry
                                                    where OINV.CardCode in (?)
                                                    and inv1.TargetType NOT IN ('14')
                                                    and inv1.ItemCode != 'AXLE'
                                                    and OINV.U_SuppGR is not NULL
                                                    and OINV.DocDate >= ? and OINV.DocDate < ?
                                                                )
                                                    select t0.partycode as "CardCode", 
                                                    STRING_AGG(convert(varchar(max),invoice), ';') WITHIN GROUP (ORDER BY docdate) AS "U_OriginalInvoiceNo",
                                                    STRING_AGG(convert(varchar(max),disdate), ';') WITHIN GROUP (ORDER BY docdate)AS "U_OriginalInvoiceDate-yyyy-mm-dd",
                                                    t0.party_partno as "SupplierCatNum",t0.tel_partno as "ItemDescription",
                                                    sum(t0.qty) as "Quantity",t0.taxcode as "TaxCode"
                                                    from details t0 where party_partno = ?
                                                    group by t0.partycode, t0.party_partno,t0.tel_partno,t0.taxcode''', card_code,sdate,edate,part_no)
                    
                            dtw_data = cur.fetchall()
                            dtw_data_list = [''.join(str(i)) for i in dtw_data]
                            
                            if len(dtw_data_list) == 0:
                                error_msg = "Error no record present in DB for DTW Details for Card code  :: {a}. Part No  :: {b}. Start Date  :: {c}. End Date {d}".format(a=card_code,b=part_no,c=sdate,d=edate)
                                raise_and_log_error(error_msg,'Dtw')
                                chk_err    = 1
                                break
                            elif len(dtw_data_list) >1:
                                error_msg = "More than 1 record present in DB for DTW Details for  Card code :: {a}. Part No  :: {b}. Start Date  :: {c}. End Date {d}".format(a=card_code,b=part_no,c=sdate,d=edate)
                                raise_and_log_error(error_msg,'Dtw')
                                chk_err    = 1
                                break
                            else :
                                dtw_cardcode = card_code
                                dtw_u_freight = 'Paid'
                                dtw_u_salescat = 'Supplementary'
                                dtw_u_dept = 'Sales & Marketing'
                                dtw_u_orig_invno = dtw_data[0][1]
                                dtw_u_orig_inv_dt = dtw_data[0][2]
                                dtw_comments = ''
                                dtw_itemcode = 'Axle'
                                dtw_suppliercatnum = dtw_data[0][3]
                                dtw_itemdescription = dtw_data[0][4]
                                dtw_qty = dtw_data[0][5]
                                dtw_taxcode = dtw_data[0][6]
                                dtw_whscode = 'FG_FBD'
                                dtw_suppdate = sdate
                                
                                inv_no_list = list(dtw_u_orig_invno.split(";"))
                                inv_date_list = list(dtw_u_orig_inv_dt.split(";"))
                                
                                single_inv_no = inv_no_list[0]
                                single_inv_date = inv_date_list[0]
                            
                                
                            #Get Price Difference
                            cur_prc_dta = cursor.execute('''select  new_po_prc,old_inv_prc ,abs(new_po_prc - old_inv_prc)as prc_diff from supp_prc_txns 
                                                            where supp_date =? and cardcode= ? and party_partno =? and status = 'A' ''' ,sdate,card_code,part_no)
                            
                            
                            
                            all_row = cur_prc_dta.fetchall()
                            all_row_list = [''.join(str(i)) for i in all_row]
                            #prc_diff_data = cur_prc_diff.fetchone()
                            if len(all_row_list) ==0:                            
                                error_msg = "Error no Data present in DB for Price Diff DTW for Card Code  :: {a}. Part No  :: {b}. Start Date  :: {c}".format(a=card_code,b=part_no,c=sdate)
                                raise_and_log_error(error_msg,'Dtw')
                                chk_err    = 1
                                break
                            elif len(all_row_list) >1:
                                error_msg = "More than 1 Price present in supp_prc_txn table for DTW for  Card code :: {a}. Part No  :: {b}. Start Date  :: {c}. End Date {d}".format(a=card_code,b=part_no,c=sdate,d=edate)
                                raise_and_log_error(error_msg,'Dtw')
                                chk_err    = 1
                                break
                            else:
                                dtw_prc_new = all_row[0][0]
                                dtw_prc_old = all_row[0][1]
                                dtw_prc_diff = all_row[0][2]
                                    
                            #Get PO NO
                            cur_po_no = cursor.execute("""WITH suppdt  AS (SELECT ordr.NumAtCard AS po_no FROM ordr INNER JOIN rdr1 ON ordr.DocEntry = rdr1.DocEntry WHERE SubCatNum = ? AND CardCode = ? AND CANCELED = 'N' AND DocStatus = 'O' AND ordr.DocDate = ( SELECT MAX(ordr.docdate) FROM ordr INNER JOIN rdr1 ON ordr.DocEntry = rdr1.DocEntry WHERE ordr.DocDate <= ? AND SubCatNum = ? AND CardCode = ? AND CANCELED = 'N' )),
                            
                            effdt AS ( SELECT ordr.NumAtCard AS po_no FROM ordr INNER JOIN rdr1 ON ordr.DocEntry = rdr1.DocEntry WHERE SubCatNum = ? AND CardCode = ? AND CANCELED = 'N' AND DocStatus = 'O' AND ordr.DocDate = ( SELECT MAX(ordr.docdate) FROM ordr INNER JOIN rdr1 ON ordr.DocEntry = rdr1.DocEntry WHERE ordr.DocDate <= ? AND SubCatNum = ? AND CardCode = ? AND CANCELED = 'N'))
                            
                            SELECT po_no FROM suppdt 
                            UNION 
                            SELECT po_no FROM effdt
                            WHERE NOT EXISTS (SELECT 1 FROM suppdt); """ ,part_no,card_code,sdate,part_no,card_code,part_no,card_code,edate,part_no,card_code)
                            
                            po_no_data = cur_po_no.fetchall()
                            po_no_data_list = [''.join(str(i)) for i in po_no_data]
                            if len(po_no_data_list) == 0:
                                error_msg = "Error no PO NO present in DB for Card Code  :: {a}. Part No  :: {b}. Start Date  :: {c}".format(a=card_code,b=part_no,c=sdate)
                                raise_and_log_error(error_msg,'Dtw')
                                chk_err    = 1
                                break
                        
                            elif len(po_no_data_list) > 1:
                                error_msg = "More than 1 PO NO present in DB for DTW for  Card code :: {a}. Part No  :: {b}. Start Date  :: {c}. End Date {d}".format(a=card_code,b=part_no,c=sdate,d=edate)
                                raise_and_log_error(error_msg,'Dtw')
                                chk_err    = 1
                                break
                                
                            else :
                                dtw_po_no = po_no_data[0][0]
                        
                            one_row =[dtw_cardcode,dtw_u_freight,dtw_u_salescat,dtw_u_dept,dtw_u_orig_invno,dtw_u_orig_inv_dt,dtw_po_no,dtw_comments,dtw_itemcode,dtw_suppliercatnum,dtw_itemdescription,dtw_qty,dtw_prc_new,dtw_prc_old,dtw_prc_diff,dtw_suppdate,dtw_taxcode,dtw_whscode,single_inv_no,single_inv_date]
                            with open(file_name, 'a+',newline = '') as f:    
                                writer = csv.writer(f) 
                                writer.writerow(map(lambda x: x, one_row))
                                f.close()
                        if(chk_err    == 0):        
                            status_upd_adt_trl(new_path_name,'PROCESSED')
                            #Email final CSV
                            msg = "DTW Excel File"
                            email(file_name,file_name,"DTW FILE",msg)
                            logger.info("DTW File WORK DONE")
                        
                    else :
                        logger.info("DTW data for SuppDate = {a} is alredy present".format( a = sdate))
                        error_msg = "DTW data for SuppDate = {a} is alredy present . Cancel the Previous AR_Invoice DTW data ".format( a = sdate)
                        raise_and_log_error(error_msg,'Dtw')
                        
                       
                except Exception as e:
                    #logger.info("EXception Block")
                    logger.exception(e)
                    conn.rollback()
                    message = "%s"%traceback.format_exc()
                    email("Error in Dtw",message)
                    logger.info(format_db_err_message(message))
                    status_upd_adt_trl(format_db_err_message(message))      
                
                finally :               
                    os.remove(file_name)
                    file_dtw_txt.close()
                    os.remove(new_path_name)
                
                
            else :
                logger.info(" DTW File for {a} is not found".format(a=i))
        
        
        if key[j] == 'Supplementary' :
            if os.path.isfile(path):
            
                cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
                split_path_name = os.path.splitext(path)[0]
                new_path_name = f'{split_path_name}{" "}{cur_ts}{".txt"}'
                os.rename(path, new_path_name)

                file_name = f"{'Supp'}{' '}{cur_ts}{'.csv'}"
                cursor = conn.cursor()
                
                file_supp_txt = open(new_path_name,"r")
                logger.info('File Location is :: {a}'.format(a=new_path_name))
            
                try:  
                    supp_txt_input = file_supp_txt.readlines()
                    supp_txt_input_conv = '%s'%supp_txt_input
                    
                    ins_adt_trl(cardcode,key[j],new_path_name,'START',supp_txt_input_conv) #inserting supplementary file details in SQL
                    
                    list_input = supp_txt_input[0].split(",")
                    logger.info("Input Data in Supplementary is  :: {a}".format(a=list_input))
                    list_count = len(list_input)
                    
                    cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
                    file_name = f"{'Supp'}{' '}{cur_ts}{'.csv'}"
                    file = open(file_name, 'w', newline='') 
                    writer = csv.writer(file)
                    fieldnames = list(parser.get(arg,"Supp.Header").split("|"))
                    writer.writerow(fieldnames)  
                    file.close()
                    
                    card_code  = list_input[0]
                    sdate      = list_input[1]
                    edate      = list_input[2]
                    chk_err    = 0
                    
                    part_list= part_list_func(list_count)
                    #logger.info("Part List in supplementary generation is {a}".format(a=part_list))   
                    
                                                            
                    cur = cursor.execute("select count(status) from supp_txns where status = 'A' and supp_date = ? ", sdate)
                    status_count = cur.fetchone()
                    
                    prc_dta = cursor.execute('''select top 1 new_po_prc,old_inv_prc  from supp_prc_txns 
                        where supp_date = ? and cardcode= ?  and status = 'A' ''' ,sdate,card_code)

                    all_row = prc_dta.fetchone()
                    all_row_list = [''.join(str(i)) for i in all_row]
                    
                    new_prc = all_row[0]
                    old_prc = all_row[1]
                    
                    if status_count[0] == 0 :
                    
                        for part in part_list:
                            
                            part_no = part

                            logger.info(part_no)

                            
                            if new_prc > old_prc :
                                cur1 = cursor.execute('''WITH OINV_data(ASN_WSN, GR_No, GR_Date,Challan_Qty, Short_Qty, Rej_Qty, GR_Qty, o_inv_no, o_Part_no, HSN_no,Vat_prcnt) AS (
                                    SELECT oinv.U_SuppWSNASN, oinv.U_SuppGR, oinv.U_SuppGRDate, inv1.U_ChallanQty,'0', oinv.U_SuppRejQty ,
                                OINV.U_SuppGRQty, oinv.DocNum,inv1.subcatnum, ochp.Dscription, inv1.VatPrcnt from OINV
                                    inner join INV1 inv1 on oinv.DocEntry = inv1.DocEntry join ochp on inv1.HsnEntry = ochp.AbsEntry
                                    where OINV.CardCode in (?)
                                    and inv1.TargetType NOT IN ('14')
                                    and inv1.ItemCode != 'AXLE'
                                    and oinv.CANCELED = 'N'
                                    and OINV.DocDate  >= ? and OINV.DocDate < ?
                                    and OINV.U_SuppGR is not NULL),
                                    
                                    tmp(subcatnum, U_OtherRefNo, DocNum,DocDate,NInv_no,NInv_date,U_OrgInvNo,U_OrgInvDate,U_SuppOldPrice,U_SuppNewPrice,Price_diff) AS
                                    (
                                                                                                        
                                        SELECT  inv1.subcatnum, U_OtherRefNo,oinv.DocNum,cast(oinv.DocDate as date),
                                            LEFT(cast(U_OrgInvNo as nvarchar(max)), CHARINDEX(';', cast(U_OrgInvNo as nvarchar(max)) + ';') - 1),
                                            LEFT(cast(U_OrgInvDate as nvarchar(max)), CHARINDEX(';', cast(U_OrgInvDate as nvarchar(max)) + ';') - 1),
                                            STUFF(cast(U_OrgInvNo as nvarchar(max)), 1, CHARINDEX(';', cast(U_OrgInvNo as nvarchar(max)) + ';'), ''),
                                            STUFF(cast(U_OrgInvDate as nvarchar(max)), 1, CHARINDEX(';', cast(U_OrgInvDate as nvarchar(max)) + ';'), ''),
                                            U_SuppOldPrice, U_SuppNewPrice, abs(U_SuppOldPrice - U_SuppNewPrice) FROM oinv join inv1 on 
                                            oinv.DocEntry = inv1.DocEntry 
                                            and u_suppdate = ?
                                            and oinv.CANCELED = 'N'                                                 
                                            and inv1.subcatnum = ?
                                        UNION all
                                        SELECT subcatnum, U_OtherRefNo, DocNum, DocDate,
                                            LEFT(U_OrgInvNo, CHARINDEX(';', U_OrgInvNo + ';') - 1),
                                            LEFT(U_OrgInvDate, CHARINDEX(';', U_OrgInvDate + ';') - 1),
                                            STUFF(U_OrgInvNo, 1, CHARINDEX(';', U_OrgInvNo + ';'), ''),
                                            STUFF(U_OrgInvDate, 1, CHARINDEX(';', U_OrgInvDate + ';'), ''),
                                            U_SuppOldPrice, U_SuppNewPrice, abs(U_SuppOldPrice - U_SuppNewPrice) FROM tmp WHERE U_OrgInvNo > '' )
                            
                                    select DocNum, DocDate, NInv_no, NInv_date, HSN_no, subcatnum, ASN_WSN, GR_No, GR_Date, U_OtherRefNo, Challan_Qty, Short_Qty, Rej_Qty, 
                                    GR_Qty,U_SuppOldPrice,U_SuppNewPrice,Price_diff,Vat_prcnt from OINV_data join tmp on OINV_data.o_inv_no = tmp.NInv_no 
                                    where subcatnum = ? OPTION (MAXRECURSION 0) ''', card_code,sdate,edate,sdate,part_no,part_no)
                                    
                            else :
                                cur1 = cursor.execute('''with tmp(subcatnum, U_OtherRefNo, DocNum,DocDate,NInv_no,NInv_date,U_OrgInvNo,U_OrgInvDate,U_SuppOldPrice,U_SuppNewPrice,Price_diff) AS
                                    (
                                                                                                        
                                        SELECT  rin1.subcatnum, U_OtherRefNo,orin.DocNum,cast(orin.DocDate as date),
                                            LEFT(cast(U_OrgInvNo as nvarchar(max)), CHARINDEX(';', cast(U_OrgInvNo as nvarchar(max)) + ';') - 1),
                                            LEFT(cast(U_OrgInvDate as nvarchar(max)), CHARINDEX(';', cast(U_OrgInvDate as nvarchar(max)) + ';') - 1),
                                            STUFF(cast(U_OrgInvNo as nvarchar(max)), 1, CHARINDEX(';', cast(U_OrgInvNo as nvarchar(max)) + ';'), ''),
                                            STUFF(cast(U_OrgInvDate as nvarchar(max)), 1, CHARINDEX(';', cast(U_OrgInvDate as nvarchar(max)) + ';'), ''),
                                            U_SuppOldPrice, U_SuppNewPrice, abs(U_SuppOldPrice - U_SuppNewPrice) FROM orin join rin1 on 
                                            orin.DocEntry = rin1.DocEntry 
                                            and u_suppdate = ?
                                            and orin.CANCELED = 'N'                                                 
                                            and rin1.subcatnum = ?
                                        UNION all
                                        SELECT subcatnum, U_OtherRefNo, DocNum, DocDate,
                                            LEFT(U_OrgInvNo, CHARINDEX(';', U_OrgInvNo + ';') - 1),
                                            LEFT(U_OrgInvDate, CHARINDEX(';', U_OrgInvDate + ';') - 1),
                                            STUFF(U_OrgInvNo, 1, CHARINDEX(';', U_OrgInvNo + ';'), ''),
                                            STUFF(U_OrgInvDate, 1, CHARINDEX(';', U_OrgInvDate + ';'), ''),
                                            U_SuppOldPrice, U_SuppNewPrice, abs(U_SuppOldPrice - U_SuppNewPrice) FROM tmp WHERE U_OrgInvNo > '' )

                                        select tmp.DocNum, tmp.DocDate, tmp.NInv_no, tmp.NInv_date, (ochp.Dscription) as HSN_no, tmp.subcatnum, oinv.U_SuppWSNASN,  oinv.U_SuppGR, oinv.U_SuppGRDate, tmp.U_OtherRefNo, 
                                        inv1.U_ChallanQty,'0' as Short_Qty,  oinv.U_SuppRejQty, 
                                        OINV.U_SuppGRQty,tmp.U_SuppOldPrice,tmp.U_SuppNewPrice,Price_diff,inv1.VatPrcnt from tmp join oinv
                                        on oinv.docnum = tmp.NInv_no 
                                                                    
                                        inner join  INV1 inv1  on oinv.DocEntry = inv1.DocEntry join ochp on inv1.HsnEntry = ochp.AbsEntry
                                        where oinv.CardCode = ?
                                        and inv1.TargetType NOT IN ('14')
                                        and inv1.ItemCode != 'AXLE'
                                        and oinv.CANCELED = 'N'
                                        and OINV.DocDate  >= ? and OINV.DocDate < ?
                                        and OINV.U_SuppGR is not NULL 
                                        and tmp.subcatnum = ? OPTION (MAXRECURSION 0)''',sdate,part_no,card_code,sdate,edate,part_no)
											
                                
                    
                            supp_data = cur1.fetchall()
                            supp_data_list = [''.join(str(i)) for i in supp_data]
                            
                            if len(supp_data_list) == 0:
                                error_msg = "Error no record present in DB for supplementary Details for Card code  :: {a}. Part No  :: {b}. Start Date  :: {c}. End Date {d}".format(a=card_code,b=part_no,c=sdate,d=edate)
                                raise_and_log_error(error_msg,'supplementary')
                                chk_err    = 1
                                break
    
                            else :
                                row_no = 0
                                for i in range(len(supp_data_list)):
                                    
                                    Supp_Rate_Diff_INV_No = supp_data[row_no][0]
                                    Supp_Rate_Diff_INV_Date = supp_data[row_no][1]
                                    Supp_Inv_No = supp_data[row_no][2]
                                    Inv_Date = supp_data[row_no][3]
                                    Supp_HSN_Code = supp_data[row_no][4]
                                    Supp_Part_Code = supp_data[row_no][5]
                                    Supp_ASN_WSN = supp_data[row_no][6]
                                    Supp_GR_No = supp_data[row_no][7]
                                    Supp_GR_Date = supp_data[row_no][8]
                                    Supp_PO_NO = supp_data[row_no][9]
                                    Supp_From = sdate
                                    Supp_To = edate
                                    Supp_Effective_Date_from = Supp_From #sdate
                                    Supp_Challan_Qty = supp_data[row_no][10]
                                    Supp_Short_Qty = supp_data[row_no][11]
                                    Supp_Rej_Qty = supp_data[row_no][12]
                                    Supp_GR_Qty = supp_data[row_no][13]
                                    Supp_Old_Price = supp_data[row_no][14]
                                    Supp_New_Price = supp_data[row_no][15]
                                    Supp_Basic_Diff = supp_data[row_no][16]
                                    Supp_Basic_Value = float(Supp_GR_Qty) * float(Supp_Basic_Diff)
                                    Supp_CGST = '0'
                                    Supp_SGST = '0'
                                    Supp_IGST = round((float(Supp_Basic_Value) * float(supp_data[row_no][17]))/100,2)
                                    Supp_Grand_Total = round(Supp_Basic_Value + Supp_IGST,2)
                                    Supp_TCS = '0'
                                    
                                    row_no = row_no+1
                                    
                                    logger.info("Supplementary Invoice date :: {a}".format(a=Inv_Date))
                                    
                                    if Inv_Date.find('-') == 4 :
                                        Supp_Inv_Date = Inv_Date
        
                                    else :
                                        formatted_inv_date =  datetime.strptime(Inv_Date,'%d-%m-%Y').date()
                                        Supp_Inv_Date = formatted_inv_date
                                        
                                    logger.info("Supplementary Invoice date will become :: {a}".format(a=Supp_Inv_Date))
                                    
                                    cursor.execute('''
                                            INSERT INTO supp_txns (cardcode, party_partno, supp_date, dr_no,dr_date,org_inv_no,org_inv_date,po_no,old_prc,new_prc,status,datecreated,dateupdated)
                                            VALUES (?,?,?,?,?,?,?,?,?,?,?,getdate(),getdate())
                                            ''',
                                            card_code,
                                            Supp_Part_Code,
                                            sdate,
                                            Supp_Rate_Diff_INV_No,
                                            Supp_Rate_Diff_INV_Date,
                                            Supp_Inv_No,
                                            Supp_Inv_Date,
                                            Supp_PO_NO,
                                            Supp_Old_Price,
                                            Supp_New_Price,
                                            'A'
                                            
                                        )
    
                                    one_row =[Supp_Rate_Diff_INV_No,Supp_Rate_Diff_INV_Date,Supp_Inv_No,Supp_Inv_Date,Supp_HSN_Code,Supp_Part_Code,Supp_ASN_WSN ,Supp_GR_No,Supp_GR_Date,Supp_PO_NO,Supp_From,Supp_To,Supp_Effective_Date_from ,Supp_Challan_Qty,Supp_Short_Qty ,Supp_Rej_Qty,Supp_GR_Qty,Supp_Old_Price,Supp_New_Price,Supp_Basic_Diff,Supp_Basic_Value ,Supp_CGST,Supp_SGST,Supp_IGST ,Supp_Grand_Total ,Supp_TCS ]
                                    with open(file_name, 'a+',newline = '') as f:    
                                        writer = csv.writer(f) 
                                        writer.writerow(map(lambda x: x, one_row))
                                        f.close() 
                                        
                                
                        
                        if(chk_err   == 0):
                            conn.commit()
                            status_upd_adt_trl(new_path_name,'PROCESSED')
                            msg = "Supplementary Excel File"
                            email(file_name,file_name,"Supplementary FILE",msg)
                            logger.info("Supplementary File WORK DONE")
                            
                   
                    else:
                        logger.info("Supplementary data in Supp_txns table for SuppDate = {a} is alredy present".format( a = sdate))
                        error_msg = "Supplementary already generated for the SuppDate = {a}. Contact Support Team to remove the data".format( a = sdate)
                        raise_and_log_error(error_msg,'Supplementary')
                       
                except Exception as e:
                    #logger.info("EXception Block")
                    logger.exception(e)
                    conn.rollback()
                    message = "%s"%traceback.format_exc()
                    email("Error in Supplementary",message)
                    logger.info(format_db_err_message(message))
                    status_upd_adt_trl(format_db_err_message(message))      
                
                finally :               

                    os.remove(file_name)
                    file_supp_txt.close()
                    os.remove(new_path_name)
                
                
            else :
                logger.info("Supplementary File for {a} is not found".format(a=i))
    
    break