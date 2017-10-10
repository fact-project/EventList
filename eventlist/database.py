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
    runType = RunType[header['RUNTYPE']].value # convert to id
    
    if not runType in [1,2]: # only process data files
        print("File: '"+ file + "' is not a data file skipping")
        return

    night = header['NIGHT']
    runId = header['RUNID']
    
    if checkIfProcessed(night, runId):
        print("File: ", file, " already processed skipping")
        return

    numEvents = header['NAXIS2']
    for i in range(numEvents):
        eventNr = table.data['EventNum'][i]
        utc = table.data['UnixTimeUTC'][i]
        eventType = table.data['TriggerType'][i]
        
        newEvent = Event(night=night, runId=runId, eventNr=eventNr, UTC=utc[0], UTCus=utc[1],
                         eventType=eventType, runType = runType)
        newEvent.save()


from zfits import FactFits
def processZFitsFile(file):
    f = FactFits(file)
    header = f['Events'].read_header()
    night = header['NIGHT']
    runId = header['RUNID']
    runType = RunType[header['RUNTYPE']].value # convert to id
    
    if checkIfProcessed(night, runId):
        print("File: ", file, " already processed skipping")
        return

    numEvents = header['ZNAXIS2']
    for i in range(numEvents):
        eventNr = f.get('Events', 'EventNum', i)
        utc = f.get('Events', 'UnixTimeUTC', i)
        eventType = f.get('Events', 'TriggerType', i)
        
        newEvent = Event(night=night, runId=runId, eventNr=eventNr, UTC=utc[0], UTCus=utc[1],
                         eventType=eventType, runType = runType)
        newEvent.save()
    

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
    #processFitsFile("/home/tarrox/physik/fact-tools/src/main/resources/testDataFile.fits.gz")
    #processZFitsFile("/home/tarrox/physik/fact-tools/src/test/resources/testDataFile.fits.fz")
    
    files = sorted(glob(rawfolder+"/**/*.fits.*", recursive=True))
    amount = len(files)

    for index, file in enumerate(files):
        print("Prozess: '"+file+"', "+str(index+1)+"/"+str(amount))
        ext = os.path.splitext(file)[1]
        if ext == ".gz":
            processFitsFile(file)
        elif ext == ".fz":
            processZFitsFile(file)
        else:
            print("Unknown extension: '"+ext+"' of file: '"+file+"', skipping")
        
        
    #checkIfProcessed(20130102,60)
    #checkIfProcessed(20130102,5)
    #checkIfProcessed(20160817,16)
