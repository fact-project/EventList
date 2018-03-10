

from eventlist.model import *
from eventlist.utils import load_config
import click

from glob import glob
from sets import Set

import logging
import sys

from fact import parse

logger = logging.getLogger('N')
logger.setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def nightRunIdToInt(night, runId):
    """
    creates a unique id for every night runId combo of the type night*1000+runid
    """
    return night*1000+runId
    
def pathDictToInt(pathDict):
    """
    Converts the path dict from the parse pyfact function into a unique number
    """
    return nightRunIdToInt(pathDict['night'], pathDict['runId'])



@click.command()
@click.argument('rawfolder', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True))
@click.option('--fs', default='isdc', type=click.Choice(ProcessingInfo.getFileSystems()), help='Which filesystem to use')
@click.option(
    '--config', '-c', envvar='EVENTLIST_CONFIG',
    help='Config file, if not given, env EVENTLIST_CONFIG and ./eventlist.yaml will be tried'
)
def updateEventlistFSStatus(rawfolder, config, fs):
    """
    Given the datafolder update the given filesystem column in the Processing DB
    Make sure to use the appropriate rawfolder for the filesystem 
    e.g. /fact/raw <-> isdc
    
    """
    logger.info("Loading config")
    if not config:
        logger.error("No config specified, can't work without it")
        return
    config, configpath = load_config(config)

    dbconfig  = config['processing_database']
    connect_processing_db(dbconfig)

    # glob all files from the raw folder and create a set of all the existing files
    filesGlob = glob(rawfolder+"/*/*/*/*.fits.?z")
    filesSet = Set([pathDictToInt(parse(x)) for x in filesGlob])
    
    query = (ProcessingInfo.select())
    
    logger.info("Reprocessing the Eventlist database for the availibility in: {}".format(fs))
    for procInfo in query:
        night = procInfo.night
        runId = procInfo.runId
        id = nightRunIdToInt(night, runId)
        # file doesn't exist anymore
        if id not in filesSet and getattr(procInfo, fs):
            setattr(procInfo,  fs,  0)
            procInfo.save()
        # file apeared again
        elif id in fileSet and not getattr(procInfo, fs):
            setattr(procInfo,  fs,  1)
            procInfo.save()
        # status of file unchanged, do nothing
    logger.info("Finisehd updating the availibility of files.")
    
