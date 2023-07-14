from com.Talbros.supplementary import utilities
from com.Talbros.supplementary import data_processing
from com.Talbros.supplementary.utilities import format_db_err_message
import sys
import os
import traceback
import getopt
import configparser
from dateutil import parser

def file_exists(directory, start,logger,customer):
    files = os.listdir(directory)
    matching_files = [file for file in files if file.startswith(start)]
    if len(matching_files) == 1:
        GrPath = os.path.join(directory, matching_files[0])
        return GrPath
    elif len(matching_files) == 0 :
        pass
    else :
        logger.info("More than one GRN File present in folder for {a}".format(a=customer))
                        
                        
#main methods
def main():
    logger = utilities.setuplogger()
    
    parser = configparser.ConfigParser()
    parser.read(r'.\supp_automation_config.properties')
    
    environment = sys.argv[ 1: ] 
    opts , args = getopt.getopt (environment, "e:" ) 
    # argument for environment
    for opt , env in opts:
        if opt in ['-e']:
            if env in [ 'LOCAL' ] or env in [ 'Local' ] or env in [ 'local' ] :
                env = 'LOCAL'
                local = parser.get(env, 'supplementary.basepath')
                basepath = local
                logger.info('Environment is :: {a}'.format(a=env))
            elif env in [ 'PROD' ] or env in [ 'Prod' ] or env in [ 'prod' ] : 
                env = 'PROD'
                prod = parser.get(env, 'supplementary.basepath')
                basepath = prod
                logger.info('Environment is :: {a}'.format(a=env))   
            elif env in [ 'UAT' ] or env in [ 'Uat' ] or env in [ 'uat' ] : 
                env = 'UAT'
                uat = parser.get(env, 'supplementary.basepath')
                basepath = uat
                logger.info('Environment is :: {a}'.format(a=env))            
    #if no argument is given exception is thrown
    if len(environment) == 0 :
        logger.exception('No Environment is selected')
        raise Exception('no environment argument is given')
        
    #dictionary values from properties file
    dictnry = eval(parser.get(env,'Supp.folder.dict'))
    logger.info('Dictionary is :: {a}'.format(a=dictnry))
    
    conn = utilities.getconnection(parser,env)
    
    for customer in dictnry: # loop on key of dictionary( customers )
        logger.info('Customer is :: {a}'.format(a=customer))
        key = dictnry[customer].split(",")
        
        for j in range(len(key)): # loop on the length of  particular key values ( GRN/Price/....)
            logger.info('File is :: {a}'.format(a=key[j]))
            path_key = f'{customer}{"."}{key[j]}{".Filename"}'
            
            path = os.path.join(basepath,customer,key[j],parser.get(env,path_key)) # original file path 
            
            if key[j] == 'Grn' : 
                filename_key = f'{customer}{"."}{key[j]}{".Filestartname"}'
                start_filename = parser.get(env, filename_key)
                GrPath = file_exists(path,start_filename,logger,customer)
                if GrPath != None :
                    try:
                        new_path_renam,new_path_name = utilities.convert_grfile_upd_adt(GrPath,logger,parser,key[j],conn,customer,basepath,env)
                        data_processing.update_grn_details(logger,parser,env,conn,new_path_renam,new_path_name,customer,key[j],basepath)
                    except Exception as e: 
                        logger.exception(e)
                        conn.rollback()
                        message = "%s"%e
                        utilities.sendemail(logger,parser,env,"Error in GRN",message)
                        utilities.status_upd_adt_trl(conn,logger,format_db_err_message(traceback.format_exc()))
                else :
                    logger.info("GRN File for {a} is not found".format(a=customer))
            if key[j] == 'Price' :    
                if os.path.isfile(path):
                    try:
                        partfulllist,cardcode_list,sdate,edate,new_path_name,file_prc_txt = utilities.read_pricefile_extract_data(path,logger,key[j],conn,customer,parser,env)
                        data_processing.process_pricedata_based_supptxns(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_prc_txt,customer,key[j])
                    except Exception as e:
                        logger.exception(e)
                        conn.rollback()
                        message = "%s"%e
                        utilities.sendemail(logger,parser,env,"Error in Price",message)
                        utilities.status_upd_adt_trl(conn,logger,format_db_err_message(traceback.format_exc()))
                else :
                    logger.info(" Text File for Price in {a} is not found".format(a=customer))              
            if key[j] == 'Dtw' :    
                if os.path.isfile(path):
                    try:
                        partfulllist,cardcode_list,sdate,edate,new_path_name,file_dtw_txt,file_name = utilities.read_dtwfile_extract_data(path,logger,key[j],conn,customer,parser,env)
                        data_processing.generate_dtw_details(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_name,customer)
                    except Exception as e:
                        logger.exception(e)
                        conn.rollback()
                        message = "%s"%e
                        utilities.sendemail(logger,parser,env,"Error in Dtw",message)
                        logger.info(format_db_err_message(message))
                        utilities.status_upd_adt_trl(conn,logger,format_db_err_message(message))
                
                    finally :               
                        os.remove(file_name)
                        file_dtw_txt.close()
                        os.remove(new_path_name)
                else :
                    logger.info(" DTW File for {a} is not found".format(a=customer)) 
            if key[j] == 'Supplementary' :    
                if os.path.isfile(path):
                    try:
                        partfulllist,cardcode_list,sdate,edate,new_path_name,file_supp_txt,file_name = utilities.read_suppfile_extract_data(path,logger,key[j],conn,customer,parser,env)
                        data_processing.process_suppdata_based_supptxns(logger,conn,cardcode_list,partfulllist,sdate,edate,env,parser,new_path_name,file_name,customer)
                        
                    except Exception as e:
                        logger.exception(e)
                        conn.rollback()
                        message = "%s"%e
                        utilities.sendemail(logger,parser,env,"Error in Supplementary",message)
                        logger.info(format_db_err_message(message))
                        utilities.status_upd_adt_trl(conn,logger,format_db_err_message(message))
                
                    finally :               
                        os.remove(file_name)
                        file_supp_txt.close()
                        os.remove(new_path_name)
                else :
                    logger.info("Supplementary File for {a} is not found".format(a=customer))   

       
        
if __name__== "__main__":
    main()

