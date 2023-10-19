from com.Talbros.supplementary.utilities import status_upd_adt_trl,sendemail,ins_adt_trl,raise_and_log_error,format_db_err_message
from datetime import datetime
import os
import pandas as pd
import shutil
import csv 
from os.path import exists as file_exists
from decimal import Decimal
import traceback 


def update_grn_details(logger,parser,env,conn,new_path_renam,new_path_name,customer,key,basepath):
    try:
        file_type_key = f'{customer}{"."}{key}{".file_type"}'
        rename_column = f'{customer}{"."}{key}{".rename_column"}'
        file_type_value = parser.get(env, file_type_key)
        #reading file as per its format
        if file_type_value == 'csv':
            df = pd.read_csv(new_path_renam, sep='\t')
        elif file_type_value == 'xlsx':
            df = pd.read_excel(new_path_renam)
        #renaming the column name 
        df.rename(columns=eval(parser.get(env, rename_column)), inplace=True)                   
        df.head() 
        #converting dateformat
        df['GR_dt'] = pd.to_datetime(df['GR_dt'],infer_datetime_format=True)                   
        #storing date after conversion
        df.to_csv(new_path_renam)
        if 'Rej_qty' not in df.columns:
            df['Rej_qty'] = 0
        else :
            df['Rej_qty'] = df['Rej_qty'].fillna('0')
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
                            ON m.docnum = t.[DC_no]""")
        
        #deleting the values of temp table 
        cursor.execute('Truncate table supp_int_data')
        #commiting changes in SQL
        conn.commit()
        logger.info('Values saved in DB')                       
        #removing file 
        os.remove(new_path_renam)                     
        #moving the main file
        mov = f'{customer}{"."}{"Grn"}{".Filename.mov"}'
        path_mov = os.path.join(basepath,customer,key,parser.get(env,mov))  # move the original  path file to processed path

        cur_ts = datetime.now().strftime('%d-%m-%Y %H-%M-%S')
        shutil.move(new_path_name, path_mov + cur_ts + ".xls")
        status_upd_adt_trl(conn,logger,path_mov + cur_ts + ".xls",'PROCESSED')
        #sending mail
        sendemail(logger,parser,env,"GRN DATA","GRN Data Updated")
        logger.info('GRN Updated')
        logger.info("GRN File WORK DONE")
    except Exception as e:                    
        logger.exception(e)
        conn.rollback()
        message = "%s"%e
        sendemail(logger,parser,env,"Error in GRN",traceback.format_exc())
        status_upd_adt_trl(conn,logger,format_db_err_message(traceback.format_exc()))
        raise Exception("There is an issue in functioning of GRN upadate process")
        
  
def process_pricedata_based_supptxns(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_prc_txt,customer,key):
    try:
        use_supptxns_key = f'{customer}{".use_supptxns"}'
        use_supptxns = parser.get(env, use_supptxns_key)
        if use_supptxns == 'Y' :
            cursor = conn.cursor()
            
            plc = ",".join(["?"] * len(cardcode_list))
            
            query1 ='''select top 1 cast(supp_date as date) from supp_txns where status = 'A' and cardcode in (%s) order by supp_date desc'''%plc
            cur = cursor.execute(query1,cardcode_list)
            supptbl_date = cur.fetchone()
            
            query2 ='''select top 1 cast(supp_date as date) from supp_prc_txns where status = 'A'  and cardcode in (%s) order by supp_date desc'''%plc
            cur = cursor.execute(query2,cardcode_list)
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
                fetch_and_insert_prices(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_prc_txt,customer,key) 
            else :
                logger.info('Supplementary is not generated')
                error_msg = "Supplementary is not generated.Please make Supplementary first for previous date "
                raise_and_log_error(conn,logger,env,parser,error_msg,'Price')
            
        elif use_supptxns == 'N' :
            fetch_and_insert_prices(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_prc_txt,customer,key)
        
    except Exception as e:                    
        logger.exception(e)
        conn.rollback()
        message = "%s"%traceback.format_exc()
        sendemail(logger,parser,env,"Error in Price",message)
        logger.info(format_db_err_message(message))
        status_upd_adt_trl(conn,logger,format_db_err_message(message))
        raise Exception(" There is problem in processing price data")  


def fetch_and_insert_prices(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_prc_txt,customer,key):
    try :
        chk_err    = 0 
        use_supptxns_key = f'{customer}{".use_supptxns"}'
        use_supptxns = parser.get(env, use_supptxns_key)
        
        cursor = conn.cursor()
        for cardcode, part_list in zip(cardcode_list, partfulllist):
            logger.info(" work for cardcode : {a}".format(a=cardcode)) 
            if chk_err  == 1 :
                break      
            for part in part_list: 
                part_no = part
                logger.info(part_no)
                
                cur = cursor.execute("""select rdr1.price from ordr inner join rdr1 on 
                                        ordr.DocEntry = rdr1.DocEntry  where CardCode = ? and SubCatNum = ? 
                                        and ordr.U_Suppdate = ? and ordr.series = '846' and CANCELED = 'N'
                                        and ordr.docdate = (select max(ordr.docdate) from ordr inner join rdr1 on 
                                        ordr.DocEntry = rdr1.DocEntry  where CardCode = ? and SubCatNum = ? 
                                        and ordr.U_Suppdate = ? and ordr.series = '846' and CANCELED = 'N')""", cardcode,part_no,sdate,cardcode,part_no,sdate)
                
                #Check null of new_prc_cur
                new_prc_cur = cur.fetchall()
                #new_prc_list = [''.join(i) for i in new_prc_cur]
                if len(new_prc_cur) == 0:
                    error_msg = "Failed to get a record from DB for New Price.Card Code is :: {a}. Part No is :: {b}. Start Date is :: {c}".format(a=cardcode,b=part_no,c=sdate)
                    raise_and_log_error(conn,logger,env,parser,error_msg,'Price')
                    chk_err    = 1
                    break                            
                
                #Duplicate PO raised
                elif len(new_prc_cur) > 1:
                    error_msg = "Duplicate PO found in DB.Card Code is :: {a}. Part No is :: {b}. Start Date is :: {c}".format(a=cardcode,b=part_no,c=sdate)
                    raise_and_log_error(conn,logger,env,parser,error_msg,'Price') 
                    chk_err    = 1
                    break
                    
                else :
                    new_prc_db =  new_prc_cur[0][0] 
                    
                #Check if for a part there are more than 1 invoice price present for date range if yes then
                #error please ask user to prepare supplementary or get 2 sets of Prices for that part from OEM.
                cur = cursor.execute("""select distinct inv1.price from INV1 inv1 inner join 
                                    OINV on oinv.DocEntry = inv1.DocEntry where  OINV.U_SuppGR is not NULL and OINV.CardCode = ? and inv1.ItemCode != 'AXLE' 
                                    and inv1.TargetType NOT IN ('14') and OINV.DocDate >= ? and OINV.DocDate < ? and oinv.CANCELED = 'N' 
                                    and inv1.subcatnum = ?""",cardcode,sdate,edate,part_no)
                
                all_row = cur.fetchall()
                price_part_list = [''.join(str(i)) for i in all_row]
                logger.info(price_part_list)
                if len(price_part_list) > 1 :
                    error_msg = "Please resolve multiple price issue for the part. Card Code is :: {a}. Part No is :: {b}. Start Date is :: {c}. End Date is {d}. For this combination, there are multiple price from invoice : {e}".format(a=cardcode,b=part_no,c=sdate,d=edate,e=price_part_list)
                    raise_and_log_error(conn,logger,env,parser,error_msg,'Price')
                    chk_err    = 1
                    break
                    
                if use_supptxns == 'Y' :
                #Get Old Price based on invoice or supp_txn table
                    cur = cursor.execute("""with inv_cte(inv_nos,inv_dt,inv_prc) as(
                                            SELECT top 1 oinv.docnum,oinv.docdate,inv1.price from INV1 inv1 inner join 
                                            OINV on oinv.DocEntry = inv1.DocEntry where OINV.CardCode = ? and inv1.ItemCode != 'AXLE' 
                                            and inv1.TargetType NOT IN ('14') and OINV.U_SuppGR is not NULL and OINV.DocDate >= ? and OINV.DocDate < ?  and oinv.CANCELED = 'N' and inv1.subcatnum = ? order by DocDate asc)
                                            select COALESCE (new_prc,inv_prc)old_inv_prc from inv_cte LEFT OUTER JOIN supp_txns 
                                            on inv_cte.inv_nos = supp_txns.org_inv_no and supp_txns.status = 'A' and supp_date >= (select max(supp_date) from supp_txns where supp_date < ? and status = 'A' and party_partno  = ?)""", cardcode,sdate,edate,part_no,sdate,part_no)              
                    old_prc_cur = cur.fetchall()
                    old_price_list = [''.join(str(i)) for i in old_prc_cur]
                    
                elif use_supptxns == 'N' :
                    cur = cursor.execute("""with inv_cte(inv_nos,inv_dt,inv_prc) as(
                                    SELECT top 1 oinv.docnum,oinv.docdate,inv1.price from INV1 inv1 inner join 
                                    OINV on oinv.DocEntry = inv1.DocEntry where OINV.CardCode = ? and inv1.ItemCode != 'AXLE' 
                                    and inv1.TargetType NOT IN ('14') and OINV.U_SuppGR is not NULL and OINV.DocDate >= ? and OINV.DocDate < ?  and oinv.CANCELED = 'N' and inv1.subcatnum = ? order by DocDate asc)
                                    select (inv_prc)old_inv_prc from inv_cte """, cardcode,sdate,edate,part_no)                      
                    old_prc_cur = cur.fetchall()
                    old_price_list = [''.join(str(i)) for i in old_prc_cur]
                    
                if len(old_price_list) == 0: 
                    error_msg = "Failed to get a record from DB for OLD Price. Card Code is :: {a}. Part No is :: {b}. Start Date is :: {c}. End Date is {d}".format(a=cardcode,b=part_no,c=sdate,d=edate)
                    raise_and_log_error(conn,logger,env,parser,error_msg,'Price')
                    chk_err    = 1
                    break  
                    
                else :
                    formatted_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    old_prc_db  =   old_prc_cur[0][0]
                    cursor.execute('''
                                    INSERT INTO supp_prc_txns (cardcode, party_partno, old_inv_prc, new_po_prc,supp_date,supp_date_end,datecreated,status)
                                    VALUES (?,?,?,?,?,?,?,?)
                                    ''',
                                    cardcode,
                                    part_no,
                                    old_prc_db,
                                    new_prc_db,
                                    sdate,
                                    edate,
                                    formatted_date,
                                    'A'
                                )
        if(chk_err == 0):
            conn.commit()
            status_upd_adt_trl(conn,logger,new_path_name,'PROCESSED')
            logger.info('Price Data Updated')
            file_prc_txt.close()
            os.remove(new_path_name)
            logger.info('Price File Work Done')
        
    except Exception as e:                    
        logger.exception(e)
        conn.rollback()
        message = "%s"%traceback.format_exc()
        sendemail(logger,parser,env,"Error in Price",message)
        logger.info(format_db_err_message(message))
        status_upd_adt_trl(conn,logger,format_db_err_message(message))
        raise Exception("There is problem in processing price data") 


        
def generate_dtw_details(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_name,customer):
    try:
        chk_err    = 0
        format_key = f'{customer}{".Dtw_format"}'
        dtw_format = parser.get(env, format_key)
        
        Dtw_Header = f'{customer}{".Dtw.Header"}'
        
        file = open(file_name, 'w', newline='') #opening csv file and getting  header name from property file
        writer = csv.writer(file)
        fieldnames = list(parser.get(env,Dtw_Header).split("|"))
        writer.writerow(fieldnames)  
        file.close()
        
        cursor = conn.cursor()
        
        plc = ",".join(["?"] * len(cardcode_list))
        args = cardcode_list
        args.append(sdate)
            
        query ='''select count(distinct u_suppdate) from oinv where  cardcode in (%s) and u_suppdate = ? and CANCELED = 'N' '''%plc
        cur = cursor.execute(query,args)

        date_count = cur.fetchone()            
        if date_count[0] == 0 :
            for cardcode, part_list in zip(cardcode_list, partfulllist):
                logger.info(" work for cardcode : {a}".format(a=cardcode)) 
                if chk_err  == 1 :
                    break      
                for part in part_list: 
                    part_no = part
                    logger.info(part_no)                                           
                    #Get Price Difference
                    cur_prc_dta = cursor.execute('''select  new_po_prc,old_inv_prc ,abs(new_po_prc - old_inv_prc)as prc_diff from supp_prc_txns 
                                                    where supp_date =? and cardcode= ? and party_partno =? and status = 'A' ''' ,sdate,cardcode,part_no)
                    
                    all_row = cur_prc_dta.fetchall()
                    all_row_list = [''.join(str(i)) for i in all_row]
                    if len(all_row_list) ==0:                            
                        error_msg = "Error no Data present in DB for Price Diff DTW for Card Code  :: {a}. Part No  :: {b}. Start Date  :: {c}".format(a=cardcode,b=part_no,c=sdate)
                        raise_and_log_error(conn,logger,env,parser,error_msg,'Dtw')
                        chk_err    = 1
                        break
                    elif len(all_row_list) >1:
                        error_msg = "More than 1 Price present in supp_prc_txn table for DTW for  Card code :: {a}. Part No  :: {b}. Start Date  :: {c}. End Date {d}".format(a=cardcode,b=part_no,c=sdate,d=edate)
                        raise_and_log_error(conn,logger,env,parser,error_msg,'Dtw')
                        chk_err    = 1
                        break
                    else:
                        dtw_prc_new = all_row[0][0]
                        dtw_prc_old = all_row[0][1]
                        dtw_prc_diff = all_row[0][2]
                            
                    #Get PO NO
                    cur_po_no = cursor.execute("""WITH suppdt  AS (SELECT ordr.NumAtCard AS po_no FROM ordr INNER JOIN rdr1 ON ordr.DocEntry =  rdr1.DocEntry WHERE SubCatNum = ? AND CardCode = ? AND CANCELED = 'N' AND DocStatus = 'O' AND ordr.DocDate = ( SELECT MAX(ordr.docdate) FROM ordr INNER JOIN rdr1 ON ordr.DocEntry = rdr1.DocEntry WHERE ordr.DocDate <= ? AND SubCatNum = ? AND CardCode = ? AND CANCELED = 'N' )),
                            
                    effdt AS ( SELECT ordr.NumAtCard AS po_no FROM ordr INNER JOIN rdr1 ON ordr.DocEntry = rdr1.DocEntry WHERE SubCatNum = ? AND CardCode = ? AND CANCELED = 'N' AND DocStatus = 'O' AND ordr.DocDate = ( SELECT MAX(ordr.docdate) FROM ordr INNER JOIN rdr1 ON ordr.DocEntry = rdr1.DocEntry WHERE ordr.DocDate <= ? AND SubCatNum = ? AND CardCode = ? AND CANCELED = 'N'))
                    
                    SELECT po_no FROM suppdt 
                    UNION 
                    SELECT po_no FROM effdt
                    WHERE NOT EXISTS (SELECT 1 FROM suppdt); """ ,part_no,cardcode,sdate,part_no,cardcode,part_no,cardcode,edate,part_no,cardcode)
                    
                    po_no_data = cur_po_no.fetchall()
                    po_no_data_list = [''.join(str(i)) for i in po_no_data]
                    if len(po_no_data_list) == 0:
                        error_msg = "Error no PO NO present in DB for Card Code  :: {a}. Part No  :: {b}. Start Date  :: {c}".format(a=cardcode,b=part_no,c=sdate)
                        raise_and_log_error(conn,logger,env,parser,error_msg,'Dtw')
                        chk_err    = 1
                        break
                
                    elif len(po_no_data_list) > 1:
                        error_msg = "More than 1 PO NO present in DB for DTW for  Card code :: {a}. Part No  :: {b}. Start Date  :: {c}. End Date {d}".format(a=cardcode,b=part_no,c=sdate,d=edate)
                        raise_and_log_error(conn,logger,env,parser,error_msg,'Dtw')
                        chk_err    = 1
                        break
                        
                    else :
                        dtw_po_no = po_no_data[0][0]
                        
                    #getting invoices as per  part no.    
                    if dtw_format == 'aggregated' :
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
                                                        group by t0.partycode, t0.party_partno,t0.tel_partno,t0.taxcode''', cardcode,sdate,edate,part_no)
                    
                    elif dtw_format == 'separated' :
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
                                                    t0.invoice AS "U_OriginalInvoiceNo",
                                                    t0.disdate AS "U_OriginalInvoiceDate-yyyy-mm-dd",
                                                    t0.party_partno as "SupplierCatNum",t0.tel_partno as "ItemDescription",
                                                    t0.qty as "Quantity",t0.taxcode as "TaxCode"
                                                    from details t0 where party_partno = ? ''', cardcode,sdate,edate,part_no)
                    dtw_data = cur.fetchall()
                    dtw_data_list = [''.join(str(i)) for i in dtw_data]
                    
                    if dtw_format == 'aggregated' :
                        if len(dtw_data_list) >1:
                            error_msg = "More than 1 record present in DB for DTW Details for  Card code :: {a}. Part No  :: {b}. Start Date  :: {c}. End Date {d}".format(a=cardcode,b=part_no,c=sdate,d=edate)
                            raise_and_log_error(conn,logger,env,parser,error_msg,'Dtw')
                            chk_err    = 1
                            break
                    
                    if len(dtw_data_list) == 0:
                        error_msg = "Error no record present in DB for DTW Details for Card code  :: {a}. Part No  :: {b}. Start Date  :: {c}. End Date {d}".format(a=cardcode,b=part_no,c=sdate,d=edate)
                        raise_and_log_error(conn,logger,env,parser,error_msg,'Dtw')
                        chk_err    = 1
                        break
                        
                    else :
                        if dtw_format == 'aggregated' : 
                            dtw_cardcode = cardcode
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
                            
                            one_row =[dtw_cardcode,dtw_u_freight,dtw_u_salescat,dtw_u_dept,dtw_u_orig_invno,dtw_u_orig_inv_dt,dtw_po_no,dtw_comments,dtw_itemcode,dtw_suppliercatnum,dtw_itemdescription,dtw_qty,dtw_prc_new,dtw_prc_old,dtw_prc_diff,dtw_suppdate,dtw_taxcode,dtw_whscode,single_inv_no,single_inv_date]
                            
                            with open(file_name, 'a+',newline = '') as f:    
                                writer = csv.writer(f) 
                                writer.writerow(map(lambda x: x, one_row))
                                f.close()
                            
                        elif dtw_format == 'separated' :
                            for row in dtw_data:
                                dtw_cardcode = cardcode
                                dtw_u_freight = 'Paid'
                                dtw_u_salescat = 'Supplementary'
                                dtw_u_dept = 'Sales & Marketing'
                                dtw_u_orig_invno = row[1]
                                dtw_u_orig_inv_dt = row[2]
                                dtw_comments = ''
                                dtw_itemcode = 'Axle'
                                dtw_suppliercatnum = row[3]
                                dtw_itemdescription = row[4]
                                dtw_qty = row[5]
                                dtw_taxcode = row[6]
                                dtw_whscode = 'FG_FBD'
                                dtw_suppdate = sdate
                                
                                one_row =[dtw_cardcode,dtw_u_freight,dtw_u_salescat,dtw_u_dept,dtw_u_orig_invno,dtw_u_orig_inv_dt,dtw_po_no,dtw_comments,dtw_itemcode,dtw_suppliercatnum,dtw_itemdescription,dtw_qty,dtw_prc_new,dtw_prc_old,dtw_prc_diff,dtw_suppdate,dtw_taxcode,dtw_whscode]
                                
                                with open(file_name, 'a+',newline = '') as f:    
                                    writer = csv.writer(f) 
                                    writer.writerow(map(lambda x: x, one_row))
                                    f.close()

            if(chk_err    == 0):        
                status_upd_adt_trl(conn,logger,new_path_name,'PROCESSED')
                #Email final CSV
                msg = "DTW Excel File"
                sendemail(logger,parser,env,file_name,file_name,"DTW FILE",msg)
                logger.info("DTW File WORK DONE")
        else :
            logger.info("DTW data for SuppDate = {a} is alredy present".format( a = sdate))
            error_msg = "DTW data for SuppDate = {a} is alredy present . Cancel the Previous AR_Invoice DTW data ".format( a = sdate)
            raise_and_log_error(conn,logger,env,parser,error_msg,'Dtw')
    except Exception as e:                    
        logger.exception(e)
        conn.rollback()
        message = "%s"%traceback.format_exc()
        sendemail(logger,parser,env,"Error in Dtw",message)
        logger.info(format_db_err_message(message))
        status_upd_adt_trl(conn,logger,format_db_err_message(message))
        raise Exception("There is problem in processing and generating Dtw") 
       

def process_suppdata_based_supptxns(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_name,customer):
    try:
        use_supptxns_key = f'{customer}{".use_supptxns"}'
        use_supptxns = parser.get(env, use_supptxns_key)
        if use_supptxns == 'Y' :
        
            cursor = conn.cursor()

            plc = ",".join(["?"] * len(cardcode_list))
            args = cardcode_list
            args.append(sdate)
            
            query ='''select count(status) from supp_txns where status = 'A' and  cardcode in (%s) and supp_date = ? '''%plc
            cur = cursor.execute(query,args)
            status_count = cur.fetchone()
        
            if status_count[0] == 0 :
                generate_supplementary_data(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_name,customer)
            else :
                logger.info("Supplementary data in Supp_txns table for SuppDate = {a} is alredy present".format( a = sdate))
                error_msg = "Supplementary already generated for the SuppDate = {a}. Contact Support Team to remove the data".format( a = sdate)
                raise_and_log_error(conn,logger,env,parser,error_msg,'Supplementary')
            
        elif use_supptxns == 'N' :
            generate_supplementary_data(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_name,customer)
        
    except Exception as e:                    
        logger.exception(e)
        conn.rollback()
        message = "%s"%traceback.format_exc()
        sendemail(logger,parser,env,"Error in Supplementary",message)
        logger.info(format_db_err_message(message))
        status_upd_adt_trl(conn,logger,format_db_err_message(message))
        raise Exception("There is problem in processing and generating Supplementary")


def generate_supplementary_data(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_name,customer):
    try :
        chk_err    = 0 
        use_supptxns_key = f'{customer}{".use_supptxns"}'
        use_supptxns = parser.get(env, use_supptxns_key)
        
        format_key = f'{customer}{".Dtw_format"}'
        dtw_format = parser.get(env, format_key)
        
        Supp_Header = f'{customer}{".Supp.Header"}'
        file = open(file_name, 'w', newline='') 
        writer = csv.writer(file)
        fieldnames = list(parser.get(env,Supp_Header).split("|"))
        writer.writerow(fieldnames)  
        file.close()
        
        cursor = conn.cursor()
       
        for cardcode, part_list in zip(cardcode_list, partfulllist):
            logger.info(" work for cardcode : {a}".format(a=cardcode)) 
            if chk_err  == 1 :
                break      
            for part in part_list: 
                part_no = part
                logger.info(part_no)
                
                prc_dta = cursor.execute('''select top 1 new_po_prc,old_inv_prc  from supp_prc_txns 
                                            where supp_date = ? and cardcode= ?  and status = 'A' ''' ,sdate,cardcode)
                all_row = prc_dta.fetchone()
                all_row_list = [''.join(str(i)) for i in all_row]
                
                new_prc = all_row[0]
                old_prc = all_row[1]
                
                if dtw_format == 'aggregated' :
                
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
                            where subcatnum = ? OPTION (MAXRECURSION 0) ''', cardcode,sdate,edate,sdate,part_no,part_no)
                            
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
                                and tmp.subcatnum = ? OPTION (MAXRECURSION 0)''',sdate,part_no,cardcode,sdate,edate,part_no)
                                    
                elif dtw_format == 'separated' : 
                
                    if new_prc > old_prc :
                        cur1 = cursor.execute('''WITH OINV_data(  docnum,DocDate, newprice,  oldprice,inv_no,subcatnum,taxcode) AS (
                                            SELECT oinv.DocNum,cast(oinv.DocDate as date),
                                            oinv.U_SuppNewPrice,oinv.U_SuppOldPrice,oinv.U_OriginalInvoiceNo ,inv1.subcatnum ,inv1.taxcode from OINV
                                            inner join INV1 inv1 on oinv.DocEntry = inv1.DocEntry 
                                            where OINV.CardCode in (?) and 
                                            oinv.U_suppdate = ?
                                            and oinv.CANCELED = 'N'                                                 
                                            and inv1.subcatnum = ?
                                            ),
            
                                            gr_data(o_inv_no,GrNo,Gr_qty) as(
                                            select oinv.docnum ,oinv.U_SuppGR,OINV.U_SuppGRQty from oinv
                                            inner join INV1 inv1 on oinv.DocEntry = inv1.DocEntry 
                                            where OINV.CardCode in (?)
                                            and inv1.TargetType NOT IN ('14')
                                            and inv1.ItemCode != 'AXLE'
                                            and oinv.CANCELED = 'N'
                                            and OINV.DocDate  >= ? and OINV.DocDate < ?
                                            and OINV.U_SuppGR is not NULL)
                                
                                
                                            select GrNo,newprice,oldprice ,DocNum, DocDate,CAST(SUBSTRING(taxcode, PATINDEX('%[0-9]%', taxcode), LEN(taxcode)) AS INT) AS numeric_taxcode,abs(oldprice - newprice)as prc_diff  ,Gr_qty
                                            from OINV_data join gr_data on OINV_data.inv_no = gr_data.o_inv_no 
                                            where subcatnum = ? ''',cardcode,sdate,part_no,cardcode,sdate,edate,part_no)
                                            
                    else:
                        cur1 = cursor.execute('''WITH ORIN_data(  docnum,DocDate, newprice,  oldprice,inv_no,subcatnum,taxcode) AS (
                                            SELECT orin.DocNum,cast(orin.DocDate as date),
                                            orin.U_SuppNewPrice,orin.U_SuppOldPrice,orin.U_OriginalInvoiceNo ,rin1.subcatnum ,rin1.taxcode from ORIN
                                            inner join RIN1  on orin.DocEntry = rin1.DocEntry 
                                            where orin.CardCode in (?) and 
                                            orin.U_suppdate = ?
                                            and orin.CANCELED = 'N'                                                 
                                            and rin1.subcatnum = ?
                                            ),
            
                                            gr_data(o_inv_no,GrNo,Gr_qty) as(
                                            select oinv.docnum ,oinv.U_SuppGR,OINV.U_SuppGRQty from oinv
                                            inner join INV1 inv1 on oinv.DocEntry = inv1.DocEntry 
                                            where OINV.CardCode in (?)
                                            and inv1.TargetType NOT IN ('14')
                                            and inv1.ItemCode != 'AXLE'
                                            and oinv.CANCELED = 'N'
                                            and OINV.DocDate  >= ? and OINV.DocDate < ?
                                            and OINV.U_SuppGR is not NULL)
                                
                                
                                            select GrNo,newprice,oldprice ,DocNum, DocDate,CAST(SUBSTRING(taxcode, PATINDEX('%[0-9]%', taxcode), LEN(taxcode)) AS INT) AS numeric_taxcode,abs(oldprice - newprice)as prc_diff  ,Gr_qty
                                            from OINV_data join gr_data on OINV_data.inv_no = gr_data.o_inv_no 
                                            where subcatnum = ? ''',cardcode,sdate,part_no,cardcode,sdate,edate,part_no)
                        
            
                supp_data = cur1.fetchall()
                supp_data_list = [''.join(str(i)) for i in supp_data]
                
                if len(supp_data_list) == 0:
                    error_msg = "Error no record present in DB for supplementary Details for Card code  :: {a}. Part No  :: {b}. Start Date  :: {c}. End Date {d}".format(a=cardcode,b=part_no,c=sdate,d=edate)
                    raise_and_log_error(conn,logger,env,parser,error_msg,'supplementary')
                    chk_err    = 1
                    break
    
                else :
                    if use_supptxns == 'Y' :
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
                                    cardcode,
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
                                
                    elif use_supptxns == 'N' :
                        for row in supp_data:
                            supp_vender_code = 'DT006'
                            supp_gr_no = row[0]
                            supp_old_price = row[1]
                            supp_new_price = row[2]
                            price_diff = row[6]
                            gr_qty = row[7]
                            supp_basic_amount = Decimal(price_diff) * Decimal(gr_qty)
                            supp_taxcode = row[5]
                            supp_invoice_amount = supp_basic_amount + (supp_basic_amount * Decimal(supp_taxcode/100))
                            supp_invoice_no = row[3]
                            supp_invoice_date = row[4]
                            
                            one_row =[supp_vender_code,supp_gr_no,supp_old_price,supp_new_price,supp_basic_amount,supp_taxcode,supp_invoice_amount,supp_invoice_no,supp_invoice_date ]
                            with open(file_name, 'a+',newline = '') as f:    
                                writer = csv.writer(f) 
                                writer.writerow(map(lambda x: x, one_row))
                                f.close()  
                    
            
        if(chk_err   == 0):
            conn.commit()
            status_upd_adt_trl(conn,logger,new_path_name,'PROCESSED')
            msg = "Supplementary Excel File"
            sendemail(logger,parser,env,file_name,file_name,"Supplementary FILE",msg)
            logger.info("Supplementary File WORK DONE")
    
    except Exception as e:                    
        logger.exception(e)
        conn.rollback()
        message = "%s"%traceback.format_exc()
        sendemail(logger,parser,env,"Error in Supplementary",message)
        logger.info(format_db_err_message(message))
        status_upd_adt_trl(conn,logger,format_db_err_message(message))
        raise Exception("There is problem in processing and generating Supplementary")
        
