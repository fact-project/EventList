import logging
import os
import pandas as pd
from enum import Enum

log = logging.getLogger(__name__)

class RunType(Enum):
    data = 1
    pedestal = 2
    ped_and_lp_ext = 11
    custom = 100

from astropy.io import fits


def processFitsFile(file):
    """
    Creates an eventlist from a fits File
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
    Creates an eventlist from a ZFitsFile
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
    Creates an eventlist of all the events in the given file and return it
    """
    ext = os.path.splitext(filename)[1]
    # basename = os.path.basename(filename)
    df = None
    if ext == ".gz":
        log.debug("Processing gz file")
        if filename[-12:] == ".drs.fits.gz":
            log.info("Drs File Skipping")
            return
        df = processFitsFile(filename)
    elif ext == ".fz":
        log.debug("Processing fz file")
        df = processZFitsFile(filename)
    else:
        log.error("Unknown extension: '"+ext+"' of file: '"+filename+"', skipping")
        return None
    return df
