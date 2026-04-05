from pathlib import Path
import csv
from typing import List, Any
import pandas as pd
import time
import datetime
from datetime import timezone
import os
import shutil
import logging
import re

utils_logger = logging.getLogger('utils_logger')
if utils_logger is None:
    utils_logger = create_logger("utils_logger",'utils.log',1, logging.INFO)

def remove_lead_trail_space(_in_string):
    """[summary]
    
    Arguments:
        _in_string {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    lstrip_string = _in_string.lstrip()
    stripped_string = lstrip_string.rstrip()
    return stripped_string

def read_csv_file(_file_name):
    """[summary]
    
    Arguments:
        _file_name {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    file_data = []
    my_file = Path(_file_name)
    if my_file.is_file():
        with open(_file_name, 'r') as csv_file:
            options_data = csv.reader(csv_file, delimiter=',', quotechar='"')
            for line in options_data:
                if line[0] != 'Symbol':
                    file_data.append(line)
    else:
        print("ERROR: File " + str(_file_name) + " does not exists \n")
    return file_data

def read_file_to_list(_file_name):
    data = list()
    file_path = Path(_file_name)
    if file_path.is_file():
   #print(_filename)
        try:
        #print(data_file)
            data_file_handle = open(file_path)
            data = data_file_handle.readlines()
            utils_logger.info("INFO: number of lines read " + str(len(data)))
            data_file_handle.close()
        except IOError:
            utils_logger.error("ERROR: file read exception")
            exit()
    return data 

def convert_list_of_lists_to_pandas(_file_data, _columns):
    """[summary]
    
    Arguments:
        _file_data {[type]} -- [description]
        _columns {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    options_dataframe = pd.DataFrame(_file_data, columns=_columns)
    return options_dataframe


def convert_date_to_timestamp(_date,_date_format):
    """[summary]
    
    Arguments:
        _date {[type]} -- [description]
        _date_format {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    if _date_format is None:
        _date_format = '%m/%d/%Y'
    try:
        timestamp = time.mktime(datetime.datetime.strptime(_date, _date_format).timetuple())
        timestamp = timestamp*1000
        return timestamp
    except (OverflowError, ValueError):
        #print("Cannot convert date " + str(_date) + " to timestamp")
        utils_logger.error("Cannot convert date " + str(_date) + "to timestamp")
        return None


def convert_timestamp_to_date(timestamp,gmt=False):
    """[summary]
    
    Arguments:
        timestamp {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    try:
        if gmt == True:
            date= datetime.datetime.fromtimestamp(int(timestamp),tz=timezone.utc)
        else:
            date= datetime.datetime.fromtimestamp(int(timestamp))
#        date_string = datetime.datetime.utcfromtimestamp(int(timestamp)).strftime('%m/%d/%y')
        return date
    except ValueError:
       utils_logger.error("Cannot convert timestamp " + str(timestamp) + " to date ")
    except Exception as e:
       print(e)
       utils_logger.error("Unknown error converting timestamp" + str(timestamp) + " to date")

def check_timestamp_is_in_future(_timestamp):
    """[summary]
    
    Arguments:
        _timestamp {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    today_timestamp = time.time()
    if _timestamp - today_timestamp > 0:
        return 0
    else:
        return -1

def compare_timestamps_date(_in_timestamp,_ref_timestamp):
    """[summary]
    
    Arguments:
        _in_timestamp {[type]} -- [description]
        _ref_timestamp {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    try:
        in_dtobject = datetime.datetime.utcfromtimestamp(int(_in_timestamp))
        if (_ref_timestamp == 0):
            ref_dtobject = datetime.datetime.now()
        else:
            ref_dtobject = datetime.datetime.utcfromtimestamp(int(_ref_timestamp))
        if ((in_dtobject.day == ref_dtobject.day) and 
           (in_dtobject.month == ref_dtobject.month) and 
           (in_dtobject.year == ref_dtobject.year)):
                return 0
        else:
            return -1
    except (OverflowError, ValueError):
        #print("Cannot convert date " + str(_date) + " to timestamp")
        utils_logger.error("Improperly formatted timestamp " + str(_in_timestamp))
        return None

def convert_to_float(string):
    """[summary]
    
    Arguments:
        string {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    try:
        value = float(string)
        return value
    except ValueError:
        print('Cannot convert string ' + str(string) + " to float")


def convert_to_int(string):
    """[summary]
    
    Arguments:
        string {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    try:
        value = int(string)
        return value
    except ValueError:
        print('Cannot convert string ' + str(string) + " to int")


def backup_file(src_dir,dst_dir, filename, include_timestamp):
    """[summary]
    
    Arguments:
        src_dir {[type]} -- [description]
        dst_dir {[type]} -- [description]
        filename {[type]} -- [description]
        include_timestamp {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    new_filename = filename
    if src_dir is None:
        src_dir = os.getcwd()
    if dst_dir is None:
        dst_dir = os.getcwd()
    src_file = os.path.join(src_dir, filename)
    base , extension = os.path.splitext(filename)
    if include_timestamp == 1:
        date_today = datetime.datetime.now()
        new_filename = str(base) + "_" + str(date_today.year) + "_" + str(date_today.month) + "_" + \
                       str(date_today.day) + "_" + str(date_today.hour) + "_" + str(date_today.minute) + "_" + \
                       str(date_today.second) + "_" + extension
    dst_file = os.path.join(dst_dir, new_filename)
    print("Backing up \n" + str(src_file) + " to \n" + str(dst_file))
    try:
        shutil.copy2(src_file, dst_file)
        return 0
    except (shutil.Error, IOError, os.error):
        print("Error : Cannot Copy file")
        return -1


def create_logger(logger_name,file_name,stream, log_level,format_level=1):
    """[summary]
    
    Arguments:
        logger_name {[type]} -- [description]
        file_name {[type]} -- [description]
        stream {[type]} -- [description]
        log_level {[type]} -- [description]
        format_level : 0 =>  %(levelname)s:%(message)s
                       1(default) => %(levelname)s:%(name)s:%(funcName)s:%(message)s
    
    Returns:
        [type] -- [description]
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    file_handler = logging.FileHandler(file_name,mode='w')
    if(format_level == 0):
        formatter = logging.Formatter("%(levelname)s | %(message)s")
    elif(format_level == 1):
        formatter = logging.Formatter("%(levelname)s | %(name)s | %(funcName)s | %(message)s")
    else:
        utils_logger.error("Incorrect format level , setting to most descriptive level 1")
        formatter = logging.Formatter("%(levelname)s | %(name)s | %(funcName)s | %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    if stream ==1:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
    return logger


def create_default_logger(logger_name):
    current_directory = os.getcwd()
    filename  = f"{current_directory}/logs/{logger_name}.log"
    utils_logger.info(f"logger created : Log file name : {filename}")
    log_level = logging.INFO
    logger  = create_logger(logger_name=logger_name, file_name=filename,stream=1,log_level=log_level,format_level=1)
    return logger

def create_hash_from_list(list_data):
    """[summary]
    
    Arguments:
        list_data {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    _dict_data = {}
    for _line in list_data:
        _key = _line[0]
        _value = _line[1]
        _dict_data[_key] = _value
    return _dict_data


def create_regex_from_extension_list(extension_list):
    """[summary]
    
    Arguments:
        extension_list {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    regex = ""
    if len(extension_list) > 0:
        regex_temp1 = ""
        for ext in extension_list:
            regex_temp1 = str(ext) + "|" + str(regex_temp1)
        regex_temp2 = re.sub( "\*", "", regex_temp1)
        regex = re.sub( "\|$", "", regex_temp2)
        return regex
    else:
        return None

def compute_new_date(_ref_date,_day_delta):
    """[summary]
    Compute date from days before and days after from satrt date 
    +ve delta will compute date "delta" days after start date 
    -ve delta will compute date "delta" days before start date 
    Arguments:
        _ref_date {[type]} -- start date 
        _day_delta {[type]} -- delta days 
    
    Returns:
        new_date -- computed date 
    """
    #ref_date_object = datetime.datetime.strptime(_ref_date,"%m/%d/%Y")
    new_date = _ref_date + datetime.timedelta(days=_day_delta)
    return new_date

def convert_date_to_string(date, format="%Y-%m-%d"):
    """
    Converts a datetime object to a string

    Args:
        date (datetime.datetime): The datetime object to be converted
        format (str, optional): The desired format of the output string. Defaults to "%Y-%m-%d".

    Returns:
        str: The converted string
    """
    if isinstance(date, datetime.datetime):
        # Use the strftime method to convert the datetime object to a string
        # The format argument specifies the format of the output string
        date_string = date.strftime(format)
        return date_string
    else:
        # If the input is not a datetime object, log an error and return None
        utils_logger.error("ERROR: Input is not datetime object" + str(type(date)))
        return None

def get_current_timestamp():
    """
    returns current timestamp 
    Returns:
        datetime :  current timestamp 
    """
    #current_timstamp = int(round(time.time()*1000))
    current_time = datetime.datetime.now()  
    #print(current_time)
    current_timestamp = current_time.timestamp()*1000
    return current_timestamp

def get_current_date():
    """
    returns current timestamp 
    Returns:
        datetime: current date
    """
    current_timestamp = get_current_timestamp()
    current_date = convert_timestamp_to_date(current_timestamp/1000)
    return current_date

def convert_string_to_date(_date,_format):
    """[summary]

    Args:
        _date ([type]): [description]
        _format ([type]): [description]

    Returns:
        [type]: [description]
    """
    date_obj = datetime.datetime.strptime(_date,_format)
    return date_obj
    

def convert_to_float(_string):
    """[summary]
    convert the string to float 
    Arguments:
        string {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    #print("trying to convert"+ str(_string))
    num_value =0
    float_val = None 
    is_negative = 1
    not_string = 0
    formatted_string = None
    try:
        formatted_string = re.sub('[a-zA-Z,]','',_string)
    except TypeError:
        not_string = 1
        num_value = _string    
    except: 
        not_string = 1

    if(not_string == 0):
        if(re.match('\+', formatted_string)) is not None:
            num_value = re.sub('\+','',formatted_string)
            is_negative =1
        elif(re.match('-', formatted_string)) is not None:
            num_value = re.sub('-','',formatted_string)
            is_negative = -1
        else:
            num_value = formatted_string
    else:
        utils_logger.error("Error running regular expression on input" + str(_string))
        return "NA"
    try:
        value = float(num_value) * is_negative
        return value
    except ValueError:
        utils_logger.error('Cannot convert string ' + str(_string) + " to float")
        return "NA"

def check_string_is_upper_case(_string):
    """[summary]
    check if all characters in string are in upper case
    Arguments:
        _string {[type]} -- [description]
    
    Returns:
        [type] -- [description]
    """
    error_code = True
    if re.search('[a-z0-9]', _string) is not None:
        error_code = False
    return error_code

def compute_file_hash(_file):
    try:
        fhandle = open(_file, "rb")
        bytes = fhandle.read()
        hash = hashlib.md5(bytes).hexdigest()
        fhandle.close()
    except IOError:
        utils_logger.error("Cannot access file " + str(_file))
    except ValueError:
        utils_logger.error("Cannot open file " + str(_file))
    except:
        utils_logger.error("unknown while opening file " + str(_file))

def compute_date_with_delta_days(_start_date_obj,_delta_days,_weekdays_only):
    days_to_be_added =0
    #print("new Algo")
    if(_delta_days < 0):
        delta_day_sign = -1
    else:
        delta_day_sign = 1
    if(_weekdays_only == 1):
        start_weekday = _start_date_obj.weekday()
        overshoot_indi = start_weekday + _delta_days
        #print(overshoot_indi)
        if overshoot_indi > 4 or overshoot_indi < 0:
            #idelta_days = abs(_delta_days)+start_weekday
            abs_delta_days = abs(_delta_days)
            
            num_weeks = int((abs_delta_days)/5)
            num_extra_days = abs_delta_days%5
            if(num_weeks == 0):
                weekend_days = (6-start_weekday)
            else:
                weekend_days = (6- start_weekday) + (num_weeks-1)*2
            days_to_be_added = (delta_day_sign)*((num_weeks*5) + weekend_days + num_extra_days)
            #print(weekend_days)
            #print(days_to_be_added)
            #print("\n\ndelta_days= " + str(_delta_days) + " idelta_days = " + str(idelta_days) + "\n num_weeks = " + str(num_weeks) + "  num_extra_days =" + str(num_extra_days) + " weekday = " + str(start_weekday) + " \ndays to be added = " + str(days_to_be_added))
        else:
            days_to_be_added = (delta_day_sign)*(_delta_days)
    else:
        days_to_be_added = (delta_day_sign)*(_delta_days)
    #print(days_to_be_added)
    new_date = compute_new_date(_start_date_obj,days_to_be_added)
    new_date_string = convert_date_to_string(new_date, "%m/%d/%Y")
    #print(new_date_string)    
    return new_date

def write_list_to_file(_filename,_data_list):
    utils_logger.info("Writing file " + str(_filename) + "\n")
    #print(_data_list)
    FILEH = open(_filename, 'w')
    for item in _data_list:
        #print(item)
        FILEH.write(item)
        FILEH.write("\n")
    FILEH.close()

def compute_date_difference(_date1, _date2, _format_string):
    #print("Start date "+ str(_date1))
    #print("Earning date "+ str(_date2))
    #print(str(_format_string))
    try:
        d1_object = datetime.datetime.strptime(_date1, "%Y-%m-%d")
    except:
        utils_logger.error("Date 1 is not in correct format " + str(_date1))
        return -1
    try:
        d2_object = datetime.datetime.strptime(_date2, "%Y-%m-%d")
    except:
        utils_logger.error("Date 2 is not in correct format " + str(_date2))
        return -1
    return abs((d2_object - d1_object).days)    

if __name__ == "__main__":
    date = "2024-06-21"
    dateObj = convert_string_to_date(date,"%Y-%m-%d")
    new_date = compute_date_with_delta_days(dateObj,1,1)
    new_date_str= convert_date_to_string(new_date,"%Y-%m-%d")
    print(new_date_str)
