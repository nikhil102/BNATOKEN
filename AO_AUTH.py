import configparser
import platform

if platform.system() == "Linux":
    from SmartApi import SmartConnect
else:
    from smartapi import SmartConnect
    
import pyotp
import time
import pprint
#import yaml
import os
import json

# LOAD CONFIG

parser = configparser.ConfigParser()
parser.read('CONFIG.ini')

API_KEY = parser.get('API_AUTH', 'API_KEY')
CLIENT_ID = parser.get('API_AUTH', 'CLIENT_ID')
EMAIL_ID = parser.get('API_AUTH', 'EMAIL_ID')
PASSWORD =  parser.get('API_AUTH', 'PASSWORD')
MPIN =  parser.get('API_AUTH', 'MPIN')
TOTP_SECURITY_KEY =  parser.get('API_AUTH', 'TOTP_SECURITY_KEY')

# LOAD CONFIG
current_path = os.getcwd()
AOAUTH_FILE_NAME = "AOAUTH"
AOAUTH_FILE_NAME_FILEPATH = os.path.join(current_path,AOAUTH_FILE_NAME + '.json')
# LOAD CONFIG

TOTP_OBJ = pyotp.TOTP(TOTP_SECURITY_KEY)
TOTP = TOTP_OBJ.now()

class ANGEL_ONE:
  def __init__(self):
     global API_KEY
     global CLIENT_ID
     global EMAIL_ID
     global PASSWORD
     global MPIN
     global TOTP_SECURITY_KEY
     global TOTP
     self.API_KEY = API_KEY
     self.CLIENT_ID = CLIENT_ID
     self.EMAIL_ID = EMAIL_ID
     self.PASSWORD = PASSWORD
     self.MPIN = MPIN
     self.TOTP_SECURITY_KEY = TOTP_SECURITY_KEY
     self.TOTP = TOTP

  def RETURN_SESSION_OBJ(self):
    self.obj = SmartConnect(api_key=self.API_KEY)
    return self.obj 
  
  def RETURN_SESSION_OBJ_AND_RESPONSE(self):
    self.obj = SmartConnect(api_key=self.API_KEY)
    self.responseSession = self.obj.generateSession(self.CLIENT_ID,self.MPIN,self.TOTP)
    return [self.obj,self.responseSession]

  def RETURN_RESPONSE(self):
    self.obj = SmartConnect(api_key=self.API_KEY)
    self.responseSession = self.obj.generateSession(self.CLIENT_ID,self.MPIN,self.TOTP)
    return self.responseSession

  def RETURN_SESSION_OBJ_AND_STORE_RESPONSE(self):
    self.obj = SmartConnect(api_key=self.API_KEY)
    self.responseSession = self.obj.generateSession(self.CLIENT_ID,self.MPIN,self.TOTP)
    with open(AOAUTH_FILE_NAME_FILEPATH, "w") as outfile:
        json.dump(self.responseSession, outfile)
    return self.obj
  
  def RETURN_SESSION_OBJ_AND_RES_AND_STORE_RES(self):
    self.obj = SmartConnect(api_key=self.API_KEY)
    self.responseSession = self.obj.generateSession(self.CLIENT_ID,self.MPIN,self.TOTP)
    with open(AOAUTH_FILE_NAME_FILEPATH, "w") as outfile:
        json.dump(self.responseSession, outfile)
    return [self.obj,self.responseSession]
     

if __name__ == "__main__": 
    ANGEL_ONE = ANGEL_ONE()
    ANGEL_ONE_OBJ = ANGEL_ONE.RETURN_SESSION_OBJ_AND_STORE_RESPONSE()
