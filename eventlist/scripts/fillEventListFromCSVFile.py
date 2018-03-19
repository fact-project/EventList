import click

from ..utils import load_config
from eventlist.scripts.eventListProcessFile import write_eventlist_into_database

from eventlist.model import *
import logging
import os
import sys

logger = logging.getLogger('EventList_CSVFile')
logger.setLevel(logging.DEBUG)
#logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

@click.command()
@click.option(
    '--config', '-c', envvar='EVENTLIST_CONFIG',
    help='Config file, if not given, env EVENTLIST_CONFIG and ./eventlist.yaml will be tried'
)
@click.option('--ignore_db', is_flag=True, help="If given, ignore if the file is missing from the processing db and just add it")
@click.argument('datafolder', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True))
def updateEventListFromCSVFile(config, ignore_db, datafolder):
    """
    Adds events into the eventlist database from a csv file
    """
    logger.info("Loading config")
    if not config:
        logger.error("No config specified, can't work without it")
        return
    config, configpath = load_config(config)

    logger.info("Connectiong to processing db")
    dbconfig = config['processing_database']
    connect_processing_db(**dbconfig)
    
    logger.debug("Get all CSV-Files")
    files = glob.glob(datafolder+"/*.csv")
    
    logger.info("Processing {} csv-files".format(len(files)))
    for path in files:
        logger.info("Process file: {}".format(path))
        df = pd.read_csv(path, index_col=False)
        basename = os.path.basename(path)
        basename = os.path.splitext(basename)[0] # remove .csv
        
        night = int(basename[:8])
        runId = int(basename[9:12])

        duplicates = df.duplicated(['night','runId','eventNr']).any()
        if duplicates:
            logger.info("An entry exists twice")
            logger.info("Set as error status and rename csv file")
            info = ProcessingInfo.get((ProcessingInfo.night==night)&(ProcessingInfo.runId==runId))
            info.status=2
            info.save()
            os.rename(path, path+".dup")
            continue
        
        logger.debug("Write events into database")
        write_eventlist_into_database(basename, night, runId, ignore_db, df)
        
        logger.debug("Removing file")
        os.remove(path)
        logger.info("  Finished File")
    
    logger.info("Finished inserting eventlist data from files")
