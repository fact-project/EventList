import peewee as pew
import click
from glob import glob
import os
import numpy as np
import pandas as pd
from fact.credentials import get_credentials

from enum import Enum

from playhouse.shortcuts import RetryOperationalError

class MyRetryDB(RetryOperationalError, pew.MySQLDatabase):
    pass

class RunType(Enum):
    data = 1
    pedestal = 2
    ped_and_lp_ext = 11
    custom = 100


dbconfig = {
    "host" : "fact-mysql.app.tu-dortmund.de",
    "database" : "eventlist",
    "user" : "fact",
    "password" : "<password>"
}
    
db = MyRetryDB(None)

class Event(pew.Model):
    night = pew.IntegerField()
    runId = pew.SmallIntegerField()
    eventNr = pew.IntegerField()
    UTC = pew.IntegerField()
    UTCus = pew.IntegerField()
    eventType = pew.SmallIntegerField()
    runType = pew.SmallIntegerField()
    
    class Meta:
        database = db
        db_table = "EventList_test"
        indexes = (
            (('night', 'runId', 'eventNr'), True),
        )
    
def checkIfProcessed(night, runId):
    try:
        Event.get((Event.night == night) & (Event.runId == runId))
    except pew.DoesNotExist:
        return False
    return True


from astropy.io import fits
def processFitsFile(file):
    hdu = fits.open(file)
    table = hdu[1]
    header = table.header
    
    runType = str(header['RUNTYPE']).strip()
    if not runType in ["data","pedestal"]: # only process data files
        print("File: '"+ file + "' is not a data file skipping, runType: '"+str(runType)+"'")
        return

    night = header['NIGHT']
    runId = header['RUNID']
    
    if checkIfProcessed(night, runId):
        print("  File: ", file, " already processed skipping")
        return

    numEvents = header['NAXIS2']
    data = []
    for i in range(numEvents):
        if i%100==0:
            print("  "+str(i)+"/"+str(numEvents))
        eventNr = table.data['EventNum'][i]
        utc = table.data['UnixTimeUTC'][i]
        eventType = table.data['TriggerType'][i]
        
        
        tmp = [night, runId, eventNr, utc[0], utc[1], eventType, RunType[runType].value]
        data.append(tmp)
    return pd.DataFrame(data, columns=["night", "runId", "eventNr","UTC", "UTCus", "eventType", "runType"])


from zfits import FactFits
def processZFitsFile(file):
    f = FactFits(file)
    header = f.header()
    
    runType = str(header['RUNTYPE']).strip()
    if not runType in ["data","pedestal"]: # only process data files
        print("  File: '"+ file + "' is not a data file skipping, runType: '"+str(runType)+"'")
        return

    night = header['NIGHT']
    runId = header['RUNID']
    
    if checkIfProcessed(night, runId):
        print("  File: ", file, " already processed skipping")
        return

    numEvents = header['ZNAXIS2']
    data = []
    for i, event in enumerate(f):
        if i%100==0:
            print("  "+str(i)+"/"+str(numEvents))
        eventNr = event['EventNum']
        utc = event['UnixTimeUTC']
        eventType = event['TriggerType']
        
        tmp = [night, runId, eventNr, utc[0], utc[1], eventType, RunType[runType].value]
        data.append(tmp)
    return pd.DataFrame(data, columns=["night", "runId", "eventNr","UTC", "UTCus", "eventType", "runType"])
    

def createTables():
    db.connect()
    db.create_tables([Event], safe=True)


def process_file(filename, outfolder=None):
    ext = os.path.splitext(filename)[1]
    df = None
    if ext == ".gz":
        if filename[-12:] == ".drs.fits.gz":
            print("  Drs File Skipping")
            return
        df = processFitsFile(filename)
    elif ext == ".fz":
        df = processZFitsFile(filename)
    else:
        print("  Unknown extension: '"+ext+"' of file: '"+filename+"', skipping")
        return
    
    if outfile:
        outfile = outfolder+"/output-"+filename+"-.csv"
        print("  Write data into file: "+outfile)
        with open(outfile, "w") as out:
            df.to_csv(out, index=False)
    else:
        print("  Insert data into DB")
        with db.atomic():
            Event.insert_many(**(data.to_dict(orient='records'))).execute()

@click.command()
@click.argument('rawfolder', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True))
@click.argument('logfile', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True))
@click.option('--outfolder', default=None, type=click.Path(exists=False, dir_okay=True, file_okay=False, readable=True),
      help="Use this output folder as the output, instead of the database.")
def fillEvents(rawfolder, logfile, outfolder):
    creds = get_credentials()
    password = dict(creds['sandbox'])['password']
    dbconfig["password"] = password
    db.init(**dbconfig)
    
    createTables()

    files = sorted(glob(rawfolder+"/**/*.fits.*", recursive=True))
    amount = len(files)
    
    
    with open(logfile,"w") as log:
        for index, file in enumerate(files):
            print("Process: '"+file+"', "+str(index+1)+"/"+str(amount))
            try:
                process_file(file, outfolder)
                pass
            except Exception as e:
                print("  Caught: "+str(type(e)))
                log.write("###File: "+file+" ###\n")
                log.write("###doc###\n")
                log.write(str(e.args))
                log.write("###end###\n")


@click.command()
@click.argument('folder', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True))
def fillEventsFromFiles(folder):
    creds = get_credentials()
    password = dict(creds['sandbox'])['password']
    dbconfig["password"] = password
    db.init(**dbconfig)    
    createTables()

    files = sorted(glob(folder+"/*.csv", recursive=True))
    
    for f in files:
        print("Read csv file: "+f)
        data = pd.read_csv(f, index_col=False)
        print("  Insert data into DB")
        with db.atomic():
            Event.insert_many(data.to_dict(orient='records')).execute()

    