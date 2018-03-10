import click

from .utils import load_config
from .data import process_data_file

from eventlist.model import *
from fact.path import parse
import logging
import os
import sys

logger = logging.getLogger('EventList_File')
logger.setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def write_eventlist_into_database(path, night, runId, ignore_db, df):
    """
    Writes the data into the eventlist database and updates the processing database
    """
    with processing_db.atomic():
        fileInfo = None
        try:
            fileInfo = ProcessingInfo.get((ProcessingInfo.night == night) & (ProcessingInfo.runId == runId))
        except pew.DoesNotExist:
            if not ignore_db:
                logger.error("The entry for the file is missing in the processing database")
                return
            else:
                logger.info("The entry for the file is missing in the processing database, adding it.")
                ext = os.path.splitext(path)[1][1:]
                fileInfo = ProcessingInfo.create(night=night, runId=runId, extension=ext, status=0)#, isdc=False, fhgfs=False,  bigtank=False)

        if fileInfo.status==1:
            logger.error("File is already processed, have you started the processing twice on this file?")
            return
        
        logger.debug("Insert Data")
        Event.insert_many(df.to_dict(orient='records')).execute()
        
        logger.debug("Update processing db")
        fileInfo.status = 1
        fileInfo.save()


def write_eventlist_into_file(path, night, runId, ignore_db, df, output_folder):
    """
    Writes the event data into a file
    """
    filename = os.path.basename(path)
    
    output_path = os.path.join(output_folder, filename+".csv")
    
    df.to_csv(output_path, index=False)

@click.command()
@click.option(
    '--config', '-c', envvar='EVENTLIST_CONFIG',
    help='Config file, if not given, env EVENTLIST_CONFIG and ./eventlist.yaml will be tried'
)
@click.option('--file', default=None, envvar='FILE',
    type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True)
)
@click.option('--ignore_db', is_flag=True, help="If given, ignore if the file is missing from the processing db and just add it")
@click.option('--out_file', envvar='OUT_FILE', default = None, help="If given wirte into a file in the data directory, given in the config (submitter.data_directory)")
def eventListProcessFile(config, file, ignore_db, out_file):
    """
    Processes a file into the EventList db or into a csv file
    """
    logger.info("Loading config")
    if not config:
        logger.error("No config specified, can't work without it")
        return
    config, configpath = load_config(config)
    
    logger.info("Processing file: '"+file+"'")

    if not os.path.exists(file):
        logger.error("File does not exists")
        return

    basename = os.path.basename(file)
    fileDict = parse(file)
    night = fileDict['night']
    runId = fileDict['run']
    logger.info("Basename: {}, Night: {}, runId: {}".format(basename, night, runId))

    logger.info("Start processing data file.")
    df = None
    df = process_data_file(file)

    if df is None:
        logger.error("Couldn't process data file")
        return
    if out_file is None:
        logger.info("Fill into database")
        dbconfig  = config['processing_database']
        connect_processing_db(**dbconfig)
        write_eventlist_into_database(file, night, runId, ignore_db, df)
    else:
        logger.info("Write data into file: "+out_file)
        output_folder = os.path.join(config['submitter']['data_directory'], "output")
        write_eventlist_into_file(file, night, runId, ignore_db, df, output_folder)

    logger.info("Finished Processing")    
