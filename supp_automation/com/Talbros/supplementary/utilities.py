import pandas as pd
import pyodbc
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import shutil
import os
import copy
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import traceback


def setuplogger() :
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(filename=r'.\supp_logfile.log', maxBytes=5*1024*1024, backupCount=5, encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    logger.info('\n')
    return logger


def getconnection(parser,env) :
   # Connection with database  ( local or production)
   conn = pyodbc.connect(Driver="%s" %parser.get(env, 'DB.Driver'),
           Server="%s" %parser.get(env, 'DB.Server'),
           PORT="%s" %parser.get(env, 'DB.PORT'),
           Database = "%s" %parser.get(env, 'DB.Database'),
           UID="%s" %parser.get(env, 'DB.UID'),
           PWD="%s" %parser.get(env, 'DB.PWD'))       
   return conn

#raise Exception and Log Exception
def raise_and_log_error(conn,logger,env,parser,error_msg,module):
    logger.info(error_msg)
    conn.rollback()
    sendemail(logger,parser,env,"Error in {a}".format(a=module),error_msg)
    status_upd_adt_trl(conn,logger,format_db_err_message(error_msg))

def format_db_err_message(message) :
    return message[:200] if len(message) > 200 else message

    
def ins_adt_trl(conn,logger,cardcode,*arg) :  
    try:        
        cursor = conn.cursor() 
        module = arg[0]
        path = arg[1]
        status = arg[2]
        txt_data = arg[3]
        cursor.execute("Insert into supp_adt_trl values(?,?,?,cast('a' as varbinary(max)),getdate(),?,NULL,?)",cardcode,module,path,status,txt_data)
        conn.commit()
        logger.info("Audit Trail Inserted for module {}".format(module))
    except Exception as e:
        conn.rollback()
        logger.exception("Failed inserting into Audit Trail {}".format(e))
        raise Exception("Failed inserting into Audit Trail {}".format(e))

# Convert file data in binary data        
def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData
        
# function to update binary data (overall file) in table
def updateBLOB(conn,logger,gr_file):
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
    
#updating path,status    
def status_upd_adt_trl(conn,logger,*arg) :
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
        #raise Exception("Updation of  Audit Trail Failed..{}".format(e))
        
        
# Email function with attachment or without attachment   sendemail     
def sendemail(logger,parser,env,*val):
    try:
        sender_email = parser.get(env,'sender')
        rec_email = parser.get(env,'receiver')
        password = parser.get(env,'password')
        
          
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
        

    
# function for part list    
def part_list_func(conn,logger,list_count,cardcode_list,sdate,edate,key,list_input):
    try:
        cursor = conn.cursor()
        Part_full_list = []
        for cardcode in cardcode_list :
            if(list_count == 4 ): # when fourth field given
                            
                input_data = list_input[3]
                
                if input_data.startswith("~"): # element not in fourth field
                    input_data = input_data.translate({ord('~'): None})
                    not_part_list = input_data.split(";")
                    not_part_list_arg = ",".join(["?"] * len(not_part_list))
                    args = not_part_list 
                    args.append(cardcode)
                    args.append(sdate)
                    args.append(edate)
                    query = """SELECT distinct inv1.SubCatNum from INV1 inv1 inner join 
                            OINV on oinv.DocEntry = inv1.DocEntry where inv1.SubCatNum not in(%s) and OINV.CardCode = ? 
                            and inv1.ItemCode != 'AXLE' and inv1.TargetType NOT IN ('14')
                            and OINV.DocDate >= ? and OINV.DocDate < ? and OINV.U_SuppGR is not NULL and oinv.CANCELED = 'N'""" %not_part_list_arg
                    cur = cursor.execute(query,args)
                    all_row = cur.fetchall()
                    part_list = [''.join(i) for i in all_row]
                    Part_full_list.append(part_list)
        
                else : # for given element in fourth field
                    part_list = list_input[3].split(";") 
                    Part_full_list.append(part_list)
                                
            if(list_count == 3 ): # fourth field not given
                query = """SELECT distinct inv1.SubCatNum from INV1 inv1 inner join 
                            OINV on oinv.DocEntry = inv1.DocEntry where OINV.CardCode = ? and inv1.ItemCode != 'AXLE' 
                            and inv1.TargetType NOT IN ('14') and OINV.DocDate >= ? and OINV.DocDate < ? and OINV.U_SuppGR is not NULL and oinv.CANCELED = 'N'""" 
                cur = cursor.execute(query,cardcode,sdate,edate)
                all_row = cur.fetchall()
                part_list = [''.join(i) for i in all_row]
                Part_full_list.append(part_list)
                
        logger.info("Part List for {b} is {a}".format(a=Part_full_list,b=key)) 
        return Part_full_list 
        
    except Exception as e:
        logger.exception(" Failed to get part list {}".format(e))
        raise Exception(" Failed to get part list {}".format(e))

def cardcode_list_func(conn,logger,customer,key,list_input): 
    try:
        cursor = conn.cursor()
        input_data = list_input[0]
        
        if input_data != '': 
        
            if input_data.startswith("~"): # element not in fourth field
                logger.info("Not cardcode List is  :: {a}".format(a= list_input[0]))
                input_data = input_data.translate({ord('~'): None})
                not_cardcode_list= input_data.split(";")
                not_cardcode_list_arg = copy.deepcopy(not_cardcode_list)
                not_cardcode_list_arg.append(customer)
                plc = ",".join(["?"] * len(not_cardcode_list))
                query = """select DISTINCT(oinv.cardcode) from ocrd inner join oinv on ocrd.cardcode = oinv.cardcode where  oinv.cardcode not in(%s) and U_cgroup = ? """ %plc
                cur = cursor.execute(query,not_cardcode_list_arg)
                all_row = cur.fetchall()
                cardcode_list = [''.join(i) for i in all_row]
                logger.info("cardcode list for which {b} has to be prepared are :{a}".format(a=cardcode_list,b=key))
                
            else  :
                cardcode_list = input_data.split(";")
                logger.info("cardcode List for {b} is {a}".format(a=cardcode_list,b=key))
                
        else :
            cur = cursor.execute( """select DISTINCT(oinv.cardcode) from ocrd inner join oinv on ocrd.cardcode = oinv.cardcode where U_cgroup = ?""",customer )
            all_row = cur.fetchall()
            cardcode_list = [''.join(i) for i in all_row]
            logger.info("cardcode List for {b} is {a}".format(a=cardcode_list,b=key))
            
        return cardcode_list
        
    except Exception as e:
        logger.exception(" Failed to get cardcode list {}".format(e))
        raise Exception(" Failed to get cardcode list {}".format(e))        

        
def convert_grfile_upd_adt(path,logger,parser,key,conn,customer_name,basepath,env) :
    try:
        cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')

        split_path_name = os.path.splitext(path)[0]
        new_path_name = f'{split_path_name}{" "}{cur_ts}{".xls"}'
        os.rename(path, new_path_name)
        
        logger.info('File Location is :: {a}'.format(a=new_path_name))
        ins_adt_trl(conn,logger,None,key,new_path_name,'START',None)
        updateBLOB(conn,logger,new_path_name)
        
        copy = f'{customer_name}{"."}{"Grn"}{".Filename.copy"}'
        path_copy = os.path.join(basepath,customer_name,key,parser.get(env,copy)) # copy the file from original path to another path 
        split_path_copy = os.path.splitext(path_copy)[0]
        new_path_copy = f'{split_path_copy}{" "}{cur_ts}{".xls"}'
        
        renam = f'{customer_name}{"."}{"Grn"}{".Filename.renam"}'
        path_renam = os.path.join(basepath,customer_name,key,parser.get(env,renam)) # rename the copied file with csv extension
        split_path_renam = os.path.splitext(path_renam)[0]
        new_path_renam = f'{split_path_renam}{" "}{cur_ts}{".csv"}'
        
        logger.info('GRN File is found')                       
        #copying file
        shutil.copy(new_path_name, new_path_copy)                        
        #changing format from xls to csv
        os.rename(new_path_copy, new_path_renam)
        status_upd_adt_trl(conn,logger,new_path_copy,'PROCESSING')
        return new_path_renam,new_path_name

    except Exception as e:  
        logger.exception(e)
        message = "%s"%e
        sendemail(logger,parser,env,"Error in Grn",message)
        logger.info(format_db_err_message(message))
        status_upd_adt_trl(conn,logger,format_db_err_message(message))
        raise Exception("There is an issue in functioning of GRN upadate process") 
        
def read_pricefile_extract_data(path,logger,key,conn,customer,parser,env) :  
    try:
        cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
        split_path_name = os.path.splitext(path)[0]
        new_path_name = f'{split_path_name}{" "}{cur_ts}{".txt"}'
        os.rename(path, new_path_name)
        
        logger.info('File Location is :: {a}'.format(a=new_path_name))
       
        # reading value fromm text file 
        file_prc_txt = open(new_path_name,"r")
        txt_input = file_prc_txt.readlines()
        txt_input_conv = '%s'%txt_input
        
        list_input = txt_input[0].split(",")
        logger.info("Input Data is  :: {a}".format(a=list_input))
        list_count = len(list_input)
        
        sdate      = list_input[1]
        edate      = list_input[2]
        
        cardcode_list = cardcode_list_func(conn,logger,customer,key,list_input)
        cardcode_list_conv = '%s'%cardcode_list
        ins_adt_trl(conn,logger,cardcode_list_conv,key,new_path_name,'START',txt_input_conv) 
        partfulllist = part_list_func(conn,logger,list_count,cardcode_list,sdate,edate,key,list_input) # calling part list function
        
        return partfulllist,cardcode_list,sdate,edate,new_path_name,file_prc_txt

    except Exception as e:
        logger.exception(e)
        message = "%s"%traceback.format_exc()
        sendemail(logger,parser,env,"Error in Price",message)
        logger.info(format_db_err_message(message))
        status_upd_adt_trl(conn,logger,format_db_err_message(message))
        raise Exception(" There is problem in processing price data") 
        
        
def read_dtwfile_extract_data(path,logger,key,conn,customer,parser,env) :
    try:
        cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
        split_path_name = os.path.splitext(path)[0]
        new_path_name = f'{split_path_name}{" "}{cur_ts}{".txt"}'
        os.rename(path, new_path_name)
        
        file_name = f"{'DTW'}{' '}{cur_ts}{'.csv'}"
        logger.info('File Location is :: {a}'.format(a=new_path_name))
    
        # reading value fromm text file 
        file_dtw_txt = open(new_path_name,"r")
        dtw_txt_input = file_dtw_txt.readlines()
        dtw_txt_input_conv = '%s'%dtw_txt_input
        
        list_input = dtw_txt_input[0].split(",")
        logger.info("Input Data in DTW is  :: {a}".format(a=list_input))
        list_count = len(list_input)
        
        sdate      = list_input[1]
        edate      = list_input[2]
        
        cardcode_list = cardcode_list_func(conn,logger,customer,key,list_input)
        cardcode_list_conv = '%s'%cardcode_list
        ins_adt_trl(conn,logger,cardcode_list_conv,key,new_path_name,'START',dtw_txt_input_conv) #inserting DTW file details in SQL
        partfulllist = part_list_func(conn,logger,list_count,cardcode_list,sdate,edate,key,list_input) # calling part list function
        
        return partfulllist,cardcode_list,sdate,edate,new_path_name,file_dtw_txt,file_name

    except Exception as e:                    
        logger.exception(e)
        message = "%s"%traceback.format_exc()
        sendemail(logger,parser,env,"Error in Dtw",message)
        logger.info(format_db_err_message(message))
        status_upd_adt_trl(conn,logger,format_db_err_message(message))
        raise Exception(" There is problem in processing and generating Dtw") 
        
        
def read_suppfile_extract_data(path,logger,key,conn,customer,parser,env) :
    try:
        cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
        split_path_name = os.path.splitext(path)[0]
        new_path_name = f'{split_path_name}{" "}{cur_ts}{".txt"}'
        os.rename(path, new_path_name)
    
        file_name = f"{'Supp'}{' '}{cur_ts}{'.csv'}"
        logger.info('File Location is :: {a}'.format(a=new_path_name))
    
        # reading value fromm text file 
        file_supp_txt = open(new_path_name,"r")
        supp_txt_input = file_supp_txt.readlines()
        supp_txt_input_conv = '%s'%supp_txt_input
        
        list_input = supp_txt_input[0].split(",")
        logger.info("Input Data in Supplementary is  :: {a}".format(a=list_input))
        list_count = len(list_input)
        sdate      = list_input[1]
        edate      = list_input[2]
        
        cardcode_list = cardcode_list_func(conn,logger,customer,key,list_input)
        cardcode_list_conv = '%s'%cardcode_list
        ins_adt_trl(conn,logger,cardcode_list_conv,key,new_path_name,'START',supp_txt_input_conv) #inserting DTW file details in SQL 
        partfulllist = part_list_func(conn,logger,list_count,cardcode_list,sdate,edate,key,list_input) # calling part list function
        
        return partfulllist,cardcode_list,sdate,edate,new_path_name,file_supp_txt,file_name

    except Exception as e:                    
        logger.exception(e)
        message = "%s"%traceback.format_exc()
        sendemail(logger,parser,env,"Error in Supplementary",message)
        logger.info(format_db_err_message(message))
        status_upd_adt_trl(conn,logger,format_db_err_message(message))
        raise Exception(" There is problem in processing and generating Supplementary") 