import peewee as pew
import click
from glob import glob
import os
import numpy as np
from fact.credentials import get_credentials

from enum import Enum
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
    
db = pew.MySQLDatabase(None)

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
        db_table = "EventList"
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
        eventNr = table.data['EventNum'][i]
        utc = table.data['UnixTimeUTC'][i]
        eventType = table.data['TriggerType'][i]
        
        
        tmp = {"night":night, "runId":runId, "eventNr":eventNr,"UTC":utc[0], "UTCus":utc[1], "eventType":eventType, "runType":RunType[runType].value}
        data.append(tmp)
        #newEvent = Event(night=night, runId=runId, eventNr=eventNr, UTC=utc[0], UTCus=utc[1],
        #                 eventType=eventType, runType = RunType[runType].value)
        #newEvent.save()
    print("  Insert data into DB")
    
    with db.atomic():
        Event.insert_many(data).execute()


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
        eventNr = event['EventNum']
        utc = event['UnixTimeUTC']
        eventType = event['TriggerType']
        
        tmp = {"night":night, "runId":runId, "eventNr":eventNr,"UTC":utc[0], "UTCus":utc[1], "eventType":eventType, "runType":RunType[runType].value}
        data.append(tmp)
        
        #newEvent = Event(night=night, runId=runId, eventNr=eventNr, UTC=utc[0], UTCus=utc[1],
        #                 eventType=eventType, runType = RunType[runType].value)
        #newEvent.save()
    
    with db.atomic():
        Event.insert_many(data).execute()

def createTables():
    db.connect()
    db.create_tables([Event], safe=True)


@click.command()
@click.argument('rawfolder', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True))
def fillEvents(rawfolder):
    creds = get_credentials()
    password = dict(creds['sandbox'])['password']
    dbconfig["password"] = password
    db.init(**dbconfig)
    
    createTables()

    files = sorted(glob(rawfolder+"/**/*.fits.*", recursive=True))
    amount = len(files)

    for index, file in enumerate(files):
        print("Process: '"+file+"', "+str(index+1)+"/"+str(amount))
        ext = os.path.splitext(file)[1]
        if ext == ".gz":
            if file[-12:] == ".drs.fits.gz":
                print("  Drs File Skipping")
                continue
            processFitsFile(file)
        elif ext == ".fz":
            processZFitsFile(file)
        else:
            print("  Unknown extension: '"+ext+"' of file: '"+file+"', skipping")

