import logging
import os
import pandas as pd

log = logging.getLogger(__name__)

from astropy.io import fits
def processFitsFile(file):
    """
    Creates a eventlist from a Fits File
    """
    hdu = fits.open(file)
    table = hdu[1]
    header = table.header
    
    runType = str(header['RUNTYPE']).strip()
    if not runType in ["data","pedestal"]: # only process data files
        log.error("File: '"+ file + "' is not a data file skipping, runType: '"+str(runType)+"'")
        return

    night = header['NIGHT']
    runId = header['RUNID']

    numEvents = header['NAXIS2']
    data = []
    for i in range(numEvents):
        if i%100==0:
            log.debug("  "+str(i)+"/"+str(numEvents))
        eventNr = table.data['EventNum'][i]
        utc = table.data['UnixTimeUTC'][i]
        eventType = table.data['TriggerType'][i]
        
        
        tmp = [night, runId, eventNr, utc[0], utc[1], eventType, RunType[runType].value]
        data.append(tmp)
    return pd.DataFrame(data, columns=["night", "runId", "eventNr","UTC", "UTCus", "eventType", "runType"])


from zfits import FactFits
def processZFitsFile(file):
    """
    Creates a eventlist from a ZFitsFile
    """
    f = FactFits(file)
    header = f.header()
    
    runType = str(header['RUNTYPE']).strip()
    if not runType in ["data","pedestal"]: # only process data files
        log.error("  File: '"+ file + "' is not a data file skipping, runType: '"+str(runType)+"'")
        return

    night = header['NIGHT']
    runId = header['RUNID']

    numEvents = header['ZNAXIS2']
    data = []
    for i, event in enumerate(f):
        if i%100==0:
            log.debug("  "+str(i)+"/"+str(numEvents))
        eventNr = event['EventNum']
        utc = event['UnixTimeUTC']
        eventType = event['TriggerType']
        
        tmp = [night, runId, eventNr, utc[0], utc[1], eventType, RunType[runType].value]
        data.append(tmp)
    return pd.DataFrame(data, columns=["night", "runId", "eventNr","UTC", "UTCus", "eventType", "runType"])
    

def process_data_file(filename):
    """
    Create a eventlist of all the events in the given file and return it
    """
    ext = os.path.splitext(filename)[1]
    basename = os.path.basename(filename)
    df = None
    if ext == ".gz":
        if filename[-12:] == ".drs.fits.gz":
            log.info("Drs File Skipping")
            return
        df = processFitsFile(filename)
    elif ext == ".fz":
        df = processZFitsFile(filename)
    else:
        log.error("Unknown extension: '"+ext+"' of file: '"+filename+"', skipping")
        return None
