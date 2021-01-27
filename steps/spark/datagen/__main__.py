import os
import random as rd
from datetime import datetime, timedelta
import string
import uuid
import sys

def dataGenBigint():
    return rd.getrandbits(32)

def dataGenBoolean():
    return rd.choice([True, False])

def dataGenSmallInt():
    return rd.getrandbits(4)

def dataGenNumeric():
    return rd.getrandbits(8)

def dataGenInteger():
    return rd.getrandbits(8)

def dataGenText():
    letters = string.ascii_letters
    result_str = ''.join(rd.choice(letters) for i in range(20))
    return result_str

def dataGenTimestamp():
    current_date=datetime.today()
    return datetime.strftime(current_date, "%Y-%m-%d %H:%M:%S")

def dataGenUUID():
    return uuid.uuid1()

def dataGenVarchar():
    letters = string.ascii_letters
    result_str = ''.join(rd.choice(letters) for i in range(8))
    return result_str

def dataGenDate():
    current_date=datetime.today()
    return datetime.strftime(current_date, "%Y-%m-%d")

def dataTypeMapping(type):
    datatypes={
        'bigint': dataGenBigint,
        'varchar': dataGenVarchar,
        'boolean': dataGenBoolean,
        'text': dataGenText,
        'timestamp': dataGenTimestamp,
        'smallint': dataGenSmallInt,
        'uuid': dataGenUUID,
        'date': dataGenDate,
        'numeric': dataGenNumeric,
        'integer': dataGenInteger
    }

    return datatypes[type]

def getschema():
    schema = {}
    return schema

def dataGenerator(logger, module_name, tablename, SimulationNum=100):
    try:
        data = []
        num = 1
        schema=getschema()
        while num <= SimulationNum:
            data.append({colname:dataTypeMapping(type)() for colname, type in schema[module_name][tablename].items()})
            num += 1
        return data
    except Exception as e:
        logger.error("Error while generating the test data because of error %s", str(e))
        sys.exit(-1)






