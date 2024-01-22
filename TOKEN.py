import os
import urllib.request
import json
import pandas as pd
from datetime import datetime, date
import platform
import gzip
import requests
from io import BytesIO
from cerberus import Validator

############ CONFIG #############

UrlBanknifty = 'https://www.nseindia.com/api/option-chain-indices?symbol=BANKNIFTY'

############ CONFIG #############

############ SCHEMA VALIDATOR #############

# Define the schema for validation
filter_Data_validtor_schema = {
    'market_open_status': {'type': 'string', 'allowed': ['YES', 'NO'], 'required': True},
    'current_expiry_date': {'type': 'date', 'required': True},
    'EXPIRYDATA': {'type': 'string', 'required': True},
    'Last_price': {'type': 'integer', 'required': True},
    'CURRENT_PRICE': {'type': 'integer', 'required': True},
    'CE_STRIKE_PRICE': {'type': 'integer', 'required': True},
    'PE_STRIKE_PRICE': {'type': 'integer', 'required': True},
    'CE_SP_UP_R_VALUE': {'type': 'integer', 'required': True},
    'CE_SP_LOW_R_VALUE': {'type': 'integer', 'required': True},
    'PE_SP_UP_R_VALUE': {'type': 'integer', 'required': True},
    'PE_SP_LOW_R_VALUE': {'type': 'integer', 'required': True},
}


############ SCHEMA VALIDATOR #############


############ ERROR MESSAGES  ######

E1 = "ERROR:current_expiry_date key not found in NSE_DATA_DICT"
E2 = "ERROR:Last_price key not found in NSE_DATA_DICT"
E2 = "ERROR:NSE_DATA_DICT IS empty return"

#FILE
E3 = "ERROR:FILE PATH NOT FOUND"
E4 = "ERROR:NSE_DATA_DICT IS empty return"
E5 = "ERROR:ANGEL ONE CUSTOM CREATE JSON FILE NOT FOUND, PLEASE CHECK FILE NOT GENRATED FOR THAT DATE"

############ ERROR MESSAGES #########

############ CORE FUNCTION  ######

def TODAYDATE_IN_TOKEN_FORMAT():
    today = date.today()
    TODAYDATE_IN_TOKEN_FORMAT = today.strftime('%d%b%Y')
    return str(TODAYDATE_IN_TOKEN_FORMAT).upper()

def DIFF_IN_DAYS_TODAY_DATE_WITH_EXPIRYDATE(EXPIRY_DATE):
    global TODAY_DATE_FORM2
    EXPIRY_AND_CURRENT_DATE_DELTA_DIFF = (EXPIRY_DATE - TODAY_DATE_FORM2)
    return EXPIRY_AND_CURRENT_DATE_DELTA_DIFF.days


def ANGEL_INSTRUMENT_LIST():
    INSTRUMENT_LIST_URL = f"https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
    response = urllib.request.urlopen(INSTRUMENT_LIST_URL)
    instrument_list = json.loads(response.read())
    return instrument_list

############ CORE FUNCTION  ######

############  GLOBAL DEFINE  ######

## INITATION
NSE_DATA_DICT = {}
current_expiry_date,EXPIRYDATA  = "",""
Last_price = 0
STRIKE_PRICE_BASE = CE_STRIKE_PRICE = PE_STRIKE_PRICE = 0
CE_SP_UP = CE_SP_LOW = PE_SP_UP = PE_SP_LOW = 0

TODAY_DATE = date.today()
DATE_FORMAT = "%Y-%m-%d"
DATE_FORMAT2 = "%d%b%Y"
DATE_FORMAT3 = "%d-%m-%Y"

TODAY_DATE_FORM1 = TODAY_DATE.strftime(DATE_FORMAT)
TODAY_DATE_FORM2 = datetime.strptime(TODAY_DATE_FORM1, DATE_FORMAT)
SCRIPT_NAME = "BANKNIFTY"
LOT_SIZE = '15'
FUT_T_DF_F_NAME = SCRIPT_NAME
FUT_T_DF_F_INST_TYPE = "FUTIDX"
FUT_T_DF_F_LOT_SIZE = LOT_SIZE
OPT_T_DF_F_NAME = SCRIPT_NAME
OPT_T_DF_F_INST_TYPE = "OPTIDX"
OPT_T_DF_F_LOT_SIZE = LOT_SIZE

current_path = os.getcwd()

TODAYDATE_IN_TOKEN_FORMAT_STR_UPPERs = TODAYDATE_IN_TOKEN_FORMAT()

DATA_DIR = os.path.join(current_path, 'DATA')
CURRENT_JSON_RES_BANKNIFTY_FILEPATH = os.path.join(DATA_DIR, 'ANGEL_BANKNIFTY_TOKENID', TODAYDATE_IN_TOKEN_FORMAT_STR_UPPERs + '.json')
SPL_DAY_FILE_PATH = os.path.join(DATA_DIR, 'SPL_DAY', 'list.csv')

STRIKE_PRICE_BASE, CE_STRIKE_PRICE, PE_STRIKE_PRICE, CE_STRIKE_PRICE_UPPER_RANGE_VALUE, CE_STRIKE_PRICE_LOWER_RANGE_VALUE, PE_STRIKE_PRICE_UPPER_RANGE_VALUE, PE_STRIKE_PRICE_LOWER_RANGE_VALUE = 0, 0, 0, 0, 0, 0, 0

############ GLOBAL DEFINE   ######


def get_data(url):
    
    url_oc = f"https://www.nseindia.com/option-chain"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36',
        'Accept-Language': 'en,gu;q=0.9,hi;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br'
    }

    try:
        with requests.Session() as sess:
            request = sess.get(url_oc, headers=headers, timeout=5)
            cookies = dict(request.cookies)
            response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
            if response.status_code == 401:
                response = sess.get(url, headers=headers, timeout=5, cookies=cookies)
            if response.status_code == 200:
                return response.text
            return ""
    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return ""

def get_data_pycurl(url):
    import pycurl
    buffer = BytesIO()
    curl = pycurl.Curl()
    headers = [
        'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language: en-US,en;q=0.5',
        'Accept-Encoding: gzip, deflate, br',
        'Connection: keep-alive',
        'Sec-Fetch-Mode: navigate',
        'Sec-Fetch-Site: cross-site',
        'Sec-Fetch-User: ?1',
        'TE: trailers',
        'Pragma: no-cache',
        'Cache-Control: no-cache',
    ]

    try:
        curl.setopt(pycurl.URL, url)
        curl.setopt(pycurl.HTTPHEADER, headers)
        curl.setopt(pycurl.WRITEDATA, buffer)
        curl.perform()
        
        response_data = gzip.decompress(buffer.getvalue()).decode('utf-8')
            
        return response_data
    
    except pycurl.error as e:
        print(f"Request failed: {e}")
        return ""
    except UnicodeDecodeError as e:
        print(f"Decoding failed: {e}")
        return ""
    finally:
        curl.close()

def FetchDataFromNSE(url_bnf):
    system = platform.system()
    response = ""
    # print(f"Running on {system}")
    if system == "Windows":
        response = get_data(url_bnf)
    elif system == "Linux":
        response = get_data_pycurl(url_bnf)  
    return response


def HOLIDAY_CHECK():
    FILE_PATH_EXIST = os.path.exists(SPL_DAY_FILE_PATH)
    if FILE_PATH_EXIST:
        SPL_DAY_DATA_FRAME = pd.read_csv(SPL_DAY_FILE_PATH)
        x_date_str = datetime.now().strftime("%d-%m-%Y")
        SPL_DAY_DATA_FRAME_FILTER = SPL_DAY_DATA_FRAME[SPL_DAY_DATA_FRAME["date"] == x_date_str]
        if not SPL_DAY_DATA_FRAME_FILTER.empty:
            first_row = SPL_DAY_DATA_FRAME_FILTER.iloc[0]
            market_open_status = first_row["market_open_status"]
            market_open_status = "YES" if market_open_status != "NO" else "NO"
            return market_open_status
    return "YES"

def get_filter_data(url_bnf):
    response = FetchDataFromNSE(url_bnf)
    
    if not response:
        return {}
    
    try:
        json_object = json.loads(response)
        records = json_object.get('records', {})
        filtered = json_object.get('filtered', {})
        
        expiry_dates = records.get('expiryDates', [])
        if not expiry_dates:
            return {}
        
        # Convert the date strings to date objects
        expiry_dates = [datetime.strptime(date_str, '%d-%b-%Y').date() for date_str in expiry_dates]


        current_expiry_date = next((date for date in expiry_dates if date > date.today()), None)
        
        if not current_expiry_date:
            return {}
        
        expiry_data = current_expiry_date.strftime('%d%b%Y').upper()
        last_price = records.get('index', {}).get('last', 0)
        
        if not last_price:
            return {}
        
        last_price = int(last_price)
        strike_price_base = int(last_price / 100)
        Holiday = HOLIDAY_CHECK()
        return {
            'market_open_status':Holiday,
            'current_expiry_date': current_expiry_date,
            'EXPIRYDATA': expiry_data,
            'Last_price': last_price,
            'CURRENT_PRICE': last_price,
            'CE_STRIKE_PRICE': strike_price_base * 100,
            'PE_STRIKE_PRICE': strike_price_base * 100 + 100,
            'CE_SP_UP_R_VALUE': strike_price_base * 100 + 1000,
            'CE_SP_LOW_R_VALUE': strike_price_base * 100 - 1000,
            'PE_SP_UP_R_VALUE': strike_price_base * 100 + 1000,
            'PE_SP_LOW_R_VALUE': strike_price_base * 100 - 1000
        }
    except json.JSONDecodeError as e:
        print(f"Failed to decode JSON: {e}")
        return {}


def ANGEL_TOKEN_JSON_DATA(FORCE=False):
    FILE_PATH_EXIST = os.path.exists(CURRENT_JSON_RES_BANKNIFTY_FILEPATH)
    # if bool(FILE_PATH_EXIST) and FORCE == False:
    #     f = open(CURRENT_JSON_RES_BANKNIFTY_FILEPATH)
    #     data = json.load(f)
    #     return data
        
    global DATE_FORMAT2
    RETURN_DICT = {}
    
    INSTRUMENT_DATA = ANGEL_INSTRUMENT_LIST()
    TOK_DF = pd.DataFrame(INSTRUMENT_DATA)
    filter_data = get_filter_data(UrlBanknifty)
    validator = Validator(filter_Data_validtor_schema)
    is_valid = validator.validate(filter_data)
    EXPIRYDATA = ""
    Last_price = 0
    if is_valid:
        EXPIRYDATA = filter_data["EXPIRYDATA"]
        Last_price = filter_data["Last_price"]
    else:
        print("Validation failed.")
        exit()
        
    global TODAY_DATE_FORM1
    global DATE_FORMAT2
    RETURN_OBJ = {}
    RETURN_OBJ['MARKET_OPEN'] = filter_data["market_open_status"]
    ANGEL_BN_FUT_TOK_DF = pd.DataFrame(TOK_DF[(TOK_DF["name"] == FUT_T_DF_F_NAME) & (TOK_DF["instrumenttype"] == FUT_T_DF_F_INST_TYPE) & (TOK_DF["lotsize"] == FUT_T_DF_F_LOT_SIZE)])
    ANGEL_BN_FUT_TOK_DF["expiry1"] = pd.to_datetime(ANGEL_BN_FUT_TOK_DF["expiry"], format=DATE_FORMAT2)
    ANGEL_BN_FUT_TOK_DF = ANGEL_BN_FUT_TOK_DF.sort_values(by="expiry1")
    RETURN_OBJ["TODAY_DATE"] = TODAY_DATE_FORM1
    if not ANGEL_BN_FUT_TOK_DF.empty:
        ANGEL_BN_FUT_FILTER_DF = pd.DataFrame(ANGEL_BN_FUT_TOK_DF[(ANGEL_BN_FUT_TOK_DF["expiry1"] >= TODAY_DATE_FORM1)])
        if not ANGEL_BN_FUT_FILTER_DF.empty:
                CURRENT_FUT_DATA = ANGEL_BN_FUT_FILTER_DF.iloc[0]
                if not CURRENT_FUT_DATA.empty:
                    CURRENT_FUT_EXPIRY_DATE = CURRENT_FUT_DATA["expiry1"]
                    
                    EXPIRY_AND_CURRENT_DATE_DELTA_DIFF_DAYS = DIFF_IN_DAYS_TODAY_DATE_WITH_EXPIRYDATE(CURRENT_FUT_EXPIRY_DATE)
                    RETURN_OBJ['MONTHLY_EX_DATE_FORM1'] = CURRENT_FUT_DATA["expiry"]
                    RETURN_OBJ['MONTHLY_EX_DATE_FORM2'] = str(CURRENT_FUT_EXPIRY_DATE)
                    RETURN_OBJ['MONTHLY_REM_DAY'] = EXPIRY_AND_CURRENT_DATE_DELTA_DIFF_DAYS
                    RETURN_OBJ['FUT_CURRENT_SYMBOL'] = CURRENT_FUT_DATA["symbol"]
                    RETURN_OBJ['FUT_CURRENT_TOKEN_NUMBER'] = CURRENT_FUT_DATA["token"]
                    RETURN_OBJ['FUT_CURRENT_PRICE'] = Last_price
                    
    if Last_price != 0 and EXPIRYDATA != "":
        ANGEL_BN_OPT_TOK_DF = TOK_DF[
            (TOK_DF["name"] == OPT_T_DF_F_NAME)
            & (TOK_DF["instrumenttype"] == OPT_T_DF_F_INST_TYPE)
            & (TOK_DF["lotsize"] == OPT_T_DF_F_LOT_SIZE)
            & (TOK_DF["expiry"] == EXPIRYDATA)
        ]
        
        df = pd.DataFrame(ANGEL_BN_OPT_TOK_DF)
        EXPIRYDATA = filter_data["EXPIRYDATA"]
        Last_price = filter_data["Last_price"]
        CE_SP_UP = filter_data["CE_SP_UP_R_VALUE"]
        CE_SP_LOW = filter_data["CE_SP_LOW_R_VALUE"]
        PE_SP_UP = filter_data["PE_SP_UP_R_VALUE"]
        PE_SP_LOW = filter_data["PE_SP_LOW_R_VALUE"]
        Last_price = int(Last_price)
        df["strike"] = pd.to_numeric(df["strike"])
        df["strike"] = df["strike"] / 100
        df["strike"] = df["strike"].astype("int64")
        df["symbol"] = df["symbol"].astype(str)
        df["ce_or_pe"] = df["symbol"].str[-2:].astype(str)
        CE_DF = df[(df["ce_or_pe"] == "CE")]
        PE_DF = df[(df["ce_or_pe"] == "PE")]
        CE_RANGE_DF = df[(df["ce_or_pe"] == "CE")]
        CE_DF = CE_DF[["token", "expiry", "strike"]]
        PE_DF = PE_DF[["token", "expiry", "strike"]]
    
        CE_RANGE_DF = CE_DF[(CE_DF["strike"] <= CE_SP_UP) & (CE_DF["strike"] >= CE_SP_LOW)]
        PE_RANGE_DF = PE_DF[(PE_DF["strike"] <= PE_SP_UP) & (PE_DF["strike"] >= PE_SP_LOW)]

        CE_RANGE_DF = CE_RANGE_DF.sort_values(by=["strike"], ignore_index=True)
        PE_RANGE_DF = PE_RANGE_DF.sort_values(by=["strike"], ignore_index=True)
        
        CE_RANGE_RAW_DICT_DF = pd.DataFrame(CE_RANGE_DF[["token","strike"]])
        PE_RANGE_RAW_DICT_DF = pd.DataFrame(PE_RANGE_DF[["token","strike"]])
    
        CE_RANGE_RAW_DICT_index_DF = CE_RANGE_RAW_DICT_DF.set_index('strike')
        PE_RANGE_RAW_DICT_index_DF = PE_RANGE_RAW_DICT_DF.set_index('strike')

        CE_RANGE_DF_DICT = CE_RANGE_RAW_DICT_index_DF.to_dict()
        PE_RANGE_DF_DICT = PE_RANGE_RAW_DICT_index_DF.to_dict()

        CE_RANGE_TOKEN_DICT = {}
        PE_RANGE_TOKEN_DICT = {}
    
        if bool(CE_RANGE_DF_DICT):
            if "token" in CE_RANGE_DF_DICT.keys():
                CE_RANGE_TOKEN_DICT = CE_RANGE_DF_DICT['token']
            
        if bool(PE_RANGE_DF_DICT):    
            if "token" in PE_RANGE_DF_DICT.keys():
                PE_RANGE_TOKEN_DICT = PE_RANGE_DF_DICT['token']

        RETURN_OBJ["OPT_CE"] = CE_RANGE_TOKEN_DICT
        RETURN_OBJ["OPT_PE"] = PE_RANGE_TOKEN_DICT
        RETURN_OBJ["OPT_EXPIRYDATA"] = EXPIRYDATA
        RETURN_OBJ["Last_price"] = Last_price
        WEEKLY_EX_DATE_FORM2 = pd.to_datetime(EXPIRYDATA, format=DATE_FORMAT2)
        WEEKLY_REM_DAY = DIFF_IN_DAYS_TODAY_DATE_WITH_EXPIRYDATE(WEEKLY_EX_DATE_FORM2)         
        RETURN_OBJ['WEEKLY_EX_DATE_FORM1'] = EXPIRYDATA
        RETURN_OBJ['WEEKLY_EX_DATE_FORM2'] = str(WEEKLY_EX_DATE_FORM2)
        RETURN_OBJ['WEEKLY_REM_DAY'] = WEEKLY_REM_DAY
        
        return RETURN_OBJ


def ANGEL_TOKEN_LIST_FILE_SAVE(FORCE=False):
    
    JSON_DATA = ANGEL_TOKEN_JSON_DATA(FORCE)
    with open(CURRENT_JSON_RES_BANKNIFTY_FILEPATH, "w") as outfile:
        json.dump(JSON_DATA, outfile)
    return JSON_DATA

if __name__ == "__main__":
         
    ANGEL_TOKEN_CUSTOM_JSON = {}
    ANGEL_TOKEN_CUSTOM_JSON = ANGEL_TOKEN_LIST_FILE_SAVE(FORCE=True)
    print(ANGEL_TOKEN_CUSTOM_JSON)
