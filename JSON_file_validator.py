from datetime import date
import os
from cerberus import Validator
import json


error_log = []

############ ERROR MESSAGES  ######

E1 = "ERROR:TODAY JSON FILE NOT FOUND"
E2 = "ERROR:JSON FILE DATA NOT Able to extract"
E2 = "ERROR:JSON FILE DATA IT SEEMS INCORRECT"

############ ERROR MESSAGES #########

def TODAYDATE_IN_TOKEN_FORMAT():
    today = date.today()
    TODAYDATE_IN_TOKEN_FORMAT = today.strftime('%d%b%Y')
    return str(TODAYDATE_IN_TOKEN_FORMAT).upper()


current_path = os.getcwd()
TODAYDATE_IN_TOKEN_FORMAT_STR_UPPERs = TODAYDATE_IN_TOKEN_FORMAT()
DATA_DIR = os.path.join(current_path, 'DATA')
ERROR_LOG_DIR = os.path.join(current_path, 'ERROR')
CURRENT_JSON_RES_BANKNIFTY_FILEPATH = os.path.join(DATA_DIR, 'ANGEL_BANKNIFTY_TOKENID', TODAYDATE_IN_TOKEN_FORMAT_STR_UPPERs + '.json')
CURRENT_ERROR_LOG_FILEPATH = os.path.join(ERROR_LOG_DIR, TODAYDATE_IN_TOKEN_FORMAT_STR_UPPERs + '.txt')


def JSON_PATH_VALIDATE():
    JSON_FILE_PATH_EXIST = os.path.exists(CURRENT_JSON_RES_BANKNIFTY_FILEPATH)
    if not bool(JSON_FILE_PATH_EXIST):
        error_log.append(E1+JSON_FILE_PATH_EXIST)
        return False
    else:
        return True  

def JSON_STR_EXTRACT():
    if JSON_PATH_VALIDATE():
        FILE_DATA = open(CURRENT_JSON_RES_BANKNIFTY_FILEPATH)
        if bool(FILE_DATA):
            FILE_JSON_LOAD_DATA = json.load(FILE_DATA)
            return FILE_JSON_LOAD_DATA
        else:
            error_log.append(E2)
            return {}
    else:
        print("NOT VALID FILE PATH")
        return {}

# Cerberus schema for validation
schema = {
    "MARKET_OPEN": {"type": "string", 'allowed': ['YES', 'NO'], "required": True},
    "TODAY_DATE": {"type": "string", "required": True},
    "MONTHLY_EX_DATE_FORM1": {"type": "string", "required": True},
    "MONTHLY_EX_DATE_FORM2": {"type": "string", "required": True},
    "MONTHLY_REM_DAY": {"type": "integer", "required": True},
    "FUT_CURRENT_SYMBOL": {"type": "string", "required": True},
    "FUT_CURRENT_TOKEN_NUMBER": {"type": "string", "required": True},
    "FUT_CURRENT_PRICE": {"type": "integer", "required": True},
    "OPT_CE": {"type": "dict", "required": True},
    "OPT_PE": {"type": "dict", "required": True},
    "OPT_EXPIRYDATA": {"type": "string", "required": True},
    "Last_price": {"type": "integer", "required": True},
    "WEEKLY_EX_DATE_FORM1": {"type": "string", "required": True},
    "WEEKLY_EX_DATE_FORM2": {"type": "string", "required": True},
    "WEEKLY_REM_DAY": {"type": "integer", "required": True},
}

# Sample JSON data
data = JSON_STR_EXTRACT()

# Create a Validator instance
validator = Validator(schema)

# Validate the JSON data
if validator.validate(data):
    print(data)
else:
    print("ERROR")
    error_log.append(E2+validator.errors)

if error_log:
    with open(CURRENT_ERROR_LOG_FILEPATH, "a") as log_file:
        log_file.write("\n".join(error_log))