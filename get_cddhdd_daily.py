'''
Created on Oct 2, 2017

@author: Joseph Randazzo
'''
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import datetime
import requests
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError
from ssl import SSLError

''' 
The 'get_noaa_content function' is responsible for querying the U.S. national oceanic and atmospheric administrations (NOAA) website for the current 7 day forecast.
To accomplish this we make a call to the NOAA's RESTful api and build an xml tree from the results. individual zip codes or a string of 100 zip codes are passed to the function 
for all 40,000 zip codes in the United States that are stored in a postgreSQL table
'''
def get_noaa_content(zipstring):
    url = "https://graphical.weather.gov/xml/sample_products/browser_interface/ndfdXMLclient.php?zipCodeList=" + zipstring + "&product=time-series&begin=2004-01-01T00:00:00&end=2019-04-21T00:00:00&maxt=maxt&mint=mint&tmpabv14d=tmpabv14d&tmpblw14d=tmpblw14d"
    noaa_ndfd_response = requests.get(url)
    noaa_xml_tree = ET.fromstring(noaa_ndfd_response.content)
    return noaa_xml_tree

''' 
The 'get_noaa_fct function' is responsible for parsing the xml tree of values retrieved from the website and splitting and extracting the relevant parts. Depending on whether
a single value or a a series of values are passed to the function, data is split into pandas data frames, pivoted, and rejoined
'''
def get_noaa_fct(frame, db_df, noaa_xml_tree):
    temps_min = []; temps_max = []; aa_prob = []; ba_prob =[];
    forecast_date = noaa_xml_tree.find(".//time-layout/start-valid-time").text
                
    for noaa_values in noaa_xml_tree.findall(".//data/parameters[@applicable-location='point" + str(frame) + "']/temperature[@type='minimum']"):
        for point_data in noaa_values.getchildren():
            temps_min.append(point_data.text)
                        
    for noaa_values in noaa_xml_tree.findall(".//data/parameters[@applicable-location='point" + str(frame) + "']/temperature[@type='maximum']"):
        for point_data in noaa_values.getchildren():
            temps_max.append(point_data.text)
                
    for noaa_values in noaa_xml_tree.findall(".//parameters[@applicable-location='point" + str(frame) + "']/climate-anomaly/weekly[@type='average temperature above normal']"):
        for point_data in noaa_values.getchildren():
            aa_prob.append(point_data.text)
                        
    for noaa_values in noaa_xml_tree.findall(".//parameters[@applicable-location='point" + str(frame) + "']/climate-anomaly/weekly[@type='average temperature below normal']"):
        for point_data in noaa_values.getchildren():
            ba_prob.append(point_data.text)        
            
    print(len(temps_max))
    print(temps_max)
    print(len(aa_prob))
    print(len(ba_prob))           
    
    if len(temps_max)!=0:
        print(len(temps_max))
        print(temps_max)            
        if len(temps_max) == 9:
            temps_max.pop(0); temps_max.pop(1); temps_min.pop(0); temps_min.pop(1); temps_min.append(0)
        else:
            if len(temps_max) == 8 and len(temps_min) == 8:
                temps_max.pop(0); temps_min.pop(0);
            else:
                temps_max.pop(0); temps_min.pop(0); temps_min.append(0)
        
        if len(aa_prob) > 0:
            aa_prob.pop(0); ba_prob.pop(0);
        else:
            aa_prob.append(0)
            ba_prob.append(0) 
        
        fct_df = pd.DataFrame({'fct_index': fct_index,'forecast_start_date': forecast_date, 'temps_min' : temps_min, 'temps_max' : temps_max})
                    
        fct_df['temps_max']= pd.to_numeric(fct_df['temps_max'], errors='coerce')
        fct_df['temps_min']= pd.to_numeric(fct_df['temps_min'], errors='coerce')
                    
        fct_df['avg_temps'] = (fct_df['temps_max'] + fct_df['temps_min']) / 2
        fct_df['cdd_hdd'] = np.where((fct_df['temps_max'] + fct_df['temps_min']) / 2 > 65,'CDD','HDD')
                    
        fct_df_min = fct_df.pivot(index='forecast_start_date', columns='fct_index', values='temps_min')
        fct_df_max = fct_df.pivot(index='forecast_start_date', columns='fct_index', values='temps_max')
        fct_df_avg = fct_df.pivot(index='forecast_start_date', columns='fct_index', values='avg_temps')
        fct_df_dd = fct_df.pivot(index='forecast_start_date', columns='fct_index', values='cdd_hdd')
                   
        fct_df_min.rename(columns={'1': 'fct_min_date_1', '2': 'fct_min_date_2', '3': 'fct_min_date_3', '4': 'fct_min_date_4', '5': 'fct_min_date_5', '6': 'fct_min_date_6', '7': 'fct_min_date_7'}, inplace=True)
        fct_df_max.rename(columns={'1': 'fct_max_date_1', '2': 'fct_max_date_2', '3': 'fct_max_date_3', '4': 'fct_max_date_4', '5': 'fct_max_date_5', '6': 'fct_max_date_6', '7': 'fct_max_date_7'}, inplace=True)
        fct_df_avg.rename(columns={'1': 'date_1_avg', '2': 'date_2_avg', '3': 'date_3_avg', '4': 'date_4_avg', '5': 'date_5_avg', '6': 'date_6_avg', '7': 'date_7_avg'}, inplace=True)
        fct_df_dd.rename(columns={'1': 'date_1_dd', '2': 'date_2_dd', '3': 'date_3_dd', '4': 'date_4_dd', '5': 'date_5_dd', '6': 'date_6_dd', '7': 'date_7_dd'}, inplace=True)
                    
        fct_df= pd.concat([fct_df_min, fct_df_max], axis=1)
        fct_df= pd.concat([fct_df, fct_df_avg], axis=1)
        fct_df= pd.concat([fct_df, fct_df_dd], axis=1)
        fct_df = fct_df.assign(aa_prob = aa_prob)
        fct_df = fct_df.assign(ba_prob = ba_prob)
        fct_df = fct_df.assign(sys_last_update=update_date)
        fct_df = fct_df.fillna('0')
    
        db_df = db_df.append(fct_df,ignore_index=False)
    return db_df

''' 
This function is responsible for writing to the log file and to the python console the status of each load. The program is designed primarily to query the website for 100 zip codes at a time. However
given that certain zip codes may be invalid from NOAA's perspective and render the retrieval useless, short intervals are queried. As such in the 'act_type' parameter, the main caller will specify whether
a short run was initiated due to invalid codes or whether a long was initiated. the 'f' variable accepts a text file to be written to 
'''
def status_response(f, act_type, status):
    f.write(act_type+" load "+status+"\n")
    f.write(str(update_date) + "\n")    
    print(act_type+" load "+status)
    print(datetime.datetime.utcnow())

''' 
The first part of the program creates a connection to the PostgreSQL database and queries for the entire listing of zip codes in the U.S. Additionally variables are created in this part of the program as follows:
    zip_codes = [] - stores a list of single zip codes that will be queried at a point in time, this list is limited by the MAX_LOAD setting
    zipstring = "" - stores a concatenated list of zip codes to be queried in batch mode
    MAX_LOAD = 100 - This setting drives the number of zip codes to be queried at any given time
    update_date = str(datetime.datetime.utcnow()) - Every row is appended with the date the program was run  
    fct_index = ['1','2','3','4','5','6','7'] - To ensure the data can be pivoted, an index is created and every min,max, avg, or degree day calculation is indexed in the application   
    f= open(update_date[:10] + " noaa_fct_daily_log.txt","a+") - This variable creates the log file when the application is run using the date the application was run
    error_zips = [] - In the event the application runs into an error, a list of zip codes that caused the error are tracked
'''  
postgres_engine = create_engine("postgresql://<DB Credentials Here>")
all_cities_dataframe = pd.read_sql("SELECT city, state, location_name, zip_code FROM qsdb_zip_codes WHERE noaa_fct_flag is null ORDER BY zip_code", postgres_engine)

zip_codes = []; zipstring = ""; MAX_LOAD = 100; update_date = str(datetime.datetime.utcnow()); fct_index = ['1','2','3','4','5','6','7']; f= open(update_date[:10] + " noaa_fct_daily_log.txt","a+"); error_zips = [];

''' 
This is the main program. Data from the NOAA's website is queried in 100 zip code blocks. When an error is encountered the program pauses and runs each zip code one at a time until it determines which zip code caused
the error. Once the zip code that caused the error is determined, it is flagged in the database table and log for further examination. This feedback mechanism allows for intelligent retrievals of data in subsequent runs
as errors are minimized and full 100 block queries can be run without encountering single errors
''' 
for row_index,row in all_cities_dataframe.iterrows():
    if row_index <= MAX_LOAD:
        zipstring = zipstring + '+' + all_cities_dataframe.iloc[row_index].iloc[3]
        zip_codes.append(all_cities_dataframe.iloc[row_index].iloc[3]) 
         
    else:
        db_df= pd.DataFrame(); zipstring = zipstring[1:]
        status_response(f,'long','started')
        zipstring = zipstring + '+' + all_cities_dataframe.iloc[row_index].iloc[3]
        zip_codes.append(all_cities_dataframe.iloc[row_index].iloc[3]) 
            
        try:
            noaa_xml_tree = get_noaa_content(zipstring)
            for frame in range (1,len(zip_codes)+1):
                db_df = get_noaa_fct(frame, db_df, noaa_xml_tree)
 
            db_df = db_df.assign(zip_code = zip_codes)

            if len(db_df.columns) > 1: 
                db_df.to_sql(name='qsdb_noaa_forecasts', con=postgres_engine, if_exists = 'append', index=True)
                
            status_response(f,'long','completed')

        except(AttributeError):
            status_response(f,'short','started')
            for zip_code in zip_codes:
                try:
                    frame = '1'
                    noaa_xml_tree = get_noaa_content(zip_code)
                    print(zipstring)
                    db_df = get_noaa_fct(frame, db_df, noaa_xml_tree)

                except(ParseError,IndexError, TypeError, AttributeError, SSLError):
                    error_zips.append(zip_code)
                    f.write("Error:" + zip_code + "\n")
                    print("Error:" + zip_code)
                    sql = "UPDATE qsdb_zip_codes SET noaa_fct_flag = 'err_flag' WHERE zip_code ='" + zip_code + "'"
                    postgres_engine.execute(sql)
                    pass
            
            for error_zip in error_zips:
                zip_codes.remove(error_zip)
                     
            db_df = db_df.assign(zip_code = zip_codes) 
            db_df.to_sql(name='qsdb_noaa_forecasts', con=postgres_engine, if_exists = 'append', index=True)
                
            db_df = pd.DataFrame([])    
            status_response(f,'short','completed')
            
        MAX_LOAD = MAX_LOAD + 100; 
        zip_codes = []; temps_min = []; temps_max = []; aa_prob = []; aa_prob =[]; zipstring = ""; temps_max=""; temps_min=""; error_zips = []

status_response(f,'----------------Daily Processing','completed')
postgres_engine.dispose();
f.close()
