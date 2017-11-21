import sys
import peewee as pew
import click
from glob import glob
import os
import subprocess as sp
import numpy as np
import pandas as pd
from fact.credentials import get_credentials
from fact.factdb import (RunInfo, RawFileAvailISDCStatus, connect_database)

from erna.automatic_processing.qsub import (get_current_jobs, build_qsub_command)

from .utils import load_config
from .data import process_data_file

import logging
import time

from enum import Enum

from playhouse.shortcuts import RetryOperationalError


logger = logging.getLogger('EventList')
logger.setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


class MyRetryDB(RetryOperationalError, pew.MySQLDatabase):
    pass


dbconfig = {
    "host" : "fact-mysql.app.tu-dortmund.de",
    "database" : "eventlist",
    "user" : "<user>",
    "password" : "<password>"
}
    
processing_db = MyRetryDB(None)

class Event(pew.Model):
    night = pew.IntegerField()
    runId = pew.SmallIntegerField()
    eventNr = pew.IntegerField()
    UTC = pew.IntegerField()
    UTCus = pew.IntegerField()
    eventType = pew.SmallIntegerField()
    runType = pew.SmallIntegerField()
    
    class Meta:
        database = processing_db
        db_table = "EventList"
        indexes = (
            (('night', 'runId', 'eventNr'), True),
        )


class ProcessStatus(Enum):
    not_processed = 0
    processed = 1
    error = 2
    
class ProcessingInfo(pew.Model):
    night = pew.IntegerField()
    runId = pew.SmallIntegerField()
    extension = pew.CharField(6)
    status = pew.SmallIntegerField()
    isdc = pew.BooleanField()
    
    class Meta:
        database = processing_db
        db_table = "File_Processing_Info"
        indexes = (
            (('night', 'runId'), True),
        )
    

def createTables():
    """
    Connect to the processing db and create the tables if they don't exist yet
    """
    processing_db.connect()
    processing_db.create_tables([Event, ProcessingInfo], safe=True)


def getAllNewFiles(limit=None):
    """
    Returns all files that are currently not part of the ProcessingInfo db
    
    @limit the maximum amount of files to read
    """
    query = (
        RunInfo.select(
            RunInfo.fnight.alias('night'),
            RunInfo.frunid.alias('runId'),
        )
        .join(RawFileAvailISDCStatus,
            on=((RunInfo.fnight==RawFileAvailISDCStatus.fnight)&(RunInfo.frunid==RawFileAvailISDCStatus.frunid))
        )
        .where(RawFileAvailISDCStatus.favailable.is_null(False))
        .where(RunInfo.froi == 300)
        .where((RunInfo.fruntypekey == 2)|(RunInfo.fruntypekey == 1))
        .where(RunInfo.fdrsstep.is_null(True))
    )
    df_isdc = pd.DataFrame(list(query.dicts()), columns=["night", "runId"])
    
    query = (
        ProcessingInfo.select(
            ProcessingInfo.night,
            ProcessingInfo.runId,
        )
    )
    df_processing = pd.DataFrame(list(query.dicts()), columns=["night","runId"])
    
    
    merged = pd.merge(df_isdc, df_processing, on=['night', 'runId'], how='left', indicator=True)
    merged = merged[merged['_merge'] == 'left_only']
    merged.drop('_merge', axis=1)
    
    if limit:
        merged = merged.head(limit)
    
    return merged
    
def getAllNotProcessedFiles():
    """
    Return all files that have yet to be processedm, on the isdc
    """
    query = (
        ProcessingInfo.select(
            ProcessingInfo.night,
            ProcessingInfo.runId,
            ProcessingInfo.extension,
        )
        .where(ProcessingInfo.status == 0)
        .where(ProcessingInfo.isdc == True)
    )
    
    df = pd.DataFrame(list(query.dicts()))
    return df

def returnPathIfExists(rawfolder, night, runId):
    """
    Creates a full path for the specific run and test wheater it is an fz or gz file and if it exists
    """
    year = night//10000
    month = (night%10000)//100
    day = night%100
    
    path = os.path.join(rawfolder, "{:04d}/{:02d}/{:02d}/{:08d}_{:03d}.fits".format(year,month,day,night,runId))
    
    if os.path.exists(path+".fz"):
        return path+".fz"
    if os.path.exists(path+".gz"):
        return path+".gz"
    return None

def getAllRunningFiles(jobs):
    """
    Get all files that are still running or are currently pending
    """
    if jobs.empty:
        return pd.DataFrame(columns=['files','night','runId'])
    
    jobs = jobs[jobs.name.str.startswith('eventlist_')]
    
    files = jobs.name.str[10:]
    night = jobs.name.str[10:18].astype(int)
    runId = jobs.name.str[19:22].astype(int)
    
    df = pd.DataFrame({'files':files, 'night':night, 'runId':runId})
    return df


def create_qsub(file, log_dir, env, kwargs):
    """
    Creates a new qsub to process a single file into the eventlist database
    """
    
    basename = os.path.basename(file)
    
    executable = sp.check_output(
        ['which', 'processFileEventList']
    ).decode().strip()
    
    env["FILE"] = file
    command = build_qsub_command(
        executable=executable,
        job_name="eventlist_"+basename,
        environment=env,
        stdout = os.path.join(log_dir, 'eventlist_{}.o'.format(basename)),
        stderr = os.path.join(log_dir, 'eventlist_{}.e'.format(basename)),
        **kwargs,
    )

    return command

@click.command()
@click.argument('rawfolder', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True))
@click.option(
    '--config', '-c', envvar='EVENTLIST_CONFIG',
    help='Config file, if not given, env EVENTLIST_CONFIG and ./eventlist.yaml will be tried'
)
@click.option(
    '--verbose', '-v', help='Set log level of "erna" to debug', is_flag=True,
)
@click.option('--no_process', is_flag=True, help='Only fill in the processing database')
@click.option('--limit', type=int, default=None,
    help='specify if the amount of new files should be limited and by how much.'
)
def processNewFiles(rawfolder, no_process, config, limit, verbose):
    """
    Processes all non processed files into the EventList db
    """

    if verbose:
        logging.getLogger('EventList').setLevel(logging.DEBUG)
    else:
        logging.getLogger('EventList').setLevel(logging.INFO)

    logger.info("Loading config")
    if not config:
        logger.error("No config specified, can't work without it")
        return
    config, configpath = load_config(config)    

    
    interval = config['submitter']['interval']
    max_queued_jobs = config['submitter']['max_queued_jobs']
    log_dir = os.path.join(config['submitter']['data_directory'], "logs")
    queue = config['submitter']['queue']
    walltime = config['submitter']['walltime']
    logger.debug("Config Data:")
    logger.debug("Interval: {}, queue: {}, max queued: {}".format(interval, queue, max_queued_jobs))
    logger.debug("walltime: {}, log dir: {}".format(walltime, log_dir))
    #os.makedirs(log_dir, exist_ok=True)

    logger.info("Connect to the databases")
    logger.debug("Connect to processing database")
    dbconfig = config['processing_database']
    processing_db.init(**dbconfig)
    
    createTables()
    
    logger.debug("Connect to fact database")
    fact_db_config = config['fact_database']
    connect_database(fact_db_config)

    logger.info("Processing all new files and yet not processed files into the db")
    logger.debug("Getting all new files")
    df = getAllNewFiles(limit)
    logger.info("Found: {} new files start processing".format(len(df)))
    
    if len(df)!=0:
        newFiles = []
        logger.debug("Prepare the new files for the database")
        for index, row in df.iterrows():
            night = row['night']
            runId = row['runId']
            path = returnPathIfExists(rawfolder, night, runId)
            if not path:
                # New file but missing on the isdc
                newFiles.append({'night':night, 'runId':runId, 'extension':"", 'status':0, 'isdc':False})
            else:
                ext = os.path.splitext(path)[1][1:]
                newFiles.append({'night':night, 'runId':runId, 'extension':ext, 'status':0, 'isdc':True})
        logger.info("Insert all new Files")
        with processing_db.atomic():
            ProcessingInfo.insert_many(newFiles).execute()
    else:
        logger.info("No new files for the processing database")

    if no_process:
        logger.info("Not processing files")
        logger.info("Finished")
        return

    logger.info("Get all unprocessed files")
    df = getAllNotProcessedFiles()
    logger.info("Found: {} unporcessed files, start processing".format(len(df)))
    
    qsub_env = {
        "WALLTIME": walltime,
        'EVENTLIST_CONFIG': configpath,
        'OUT_FILE': 'True'
    }
    
    qsub_kwargs = {
        'mail_address' : config['submitter']['mail_address'],
        'mail_settings' : config['submitter']['mail_settings'],
        'queue' : queue,
    }

    logger.info("Process all unprocessed files")
    try:
        for index, row in df.iterrows():
            if limit is not None:
                if index==limit:
                    logger.info("Reached allowed limit of files to process")
                    break
            night = row['night']
            runId = row['runId']
            
            ext = row['extension']
            year = night//10000
            month = (night%10000)//100
            day = night%100
        
            path = os.path.join(rawfolder, "{:04d}/{:02d}/{:02d}/{:08d}_{:03d}.fits.{}".format(year, month, day, night, runId, ext))
            logger.info("Processing night: {}, runId:{}".format(night, runId))
            logger.info("  Path: "+path);
            
            logger.info("Get all still running or pending files")
            current_jobs = get_current_jobs()
            runningFiles = getAllRunningFiles(current_jobs)
            # TODO check for finished files here also
            if ((runningFiles['night'] == night) & (runningFiles['runId'] == runId)).any():
                logger.info("File already in processing skipping")
                continue
            
            # create qsub command
            qsub_cmd = create_qsub(path, log_dir, qsub_env, qsub_kwargs)
            
            logger.debug("Qsub command:")
            logger.debug(qsub_cmd)
            # execute
            while True:
                pending_jobs = current_jobs.query('state == "pending"')
                if len(pending_jobs) < max_queued_jobs:
                    logger.info("Sending to qsub")
                    output = sp.check_output(qsub_cmd)
                    logger.debug(output.decode().strip())
                    break
                logger.debug("Wait for jobs to clear up: {}/{}".format(len(pending_jobs), max_queued_jobs))
                time.sleep(interval)
                current_jobs = get_current_jobs()
            time.sleep(interval)
            
    except (KeyboardInterrupt, SystemExit):
        logger.info('Shutting done')
        logger.info('Clean up running jobs')
        current_jobs = get_current_jobs()
        myjobs = current_jobs[current_jobs.name.str.startswith('eventlist_')]
        logger.info("Removing {} jobs".format(len(myjobs)))
        for index, job in myjobs.iterrows():
            logger.debug("Close job: {}".format(job['name']))
            sp.run(['qdel', job['name']])
    logger.info("Finished")
        



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
                fileInfo = ProcessingInfo.create(night=night, runId=runId, extension=ext, status=0, isdc=True)

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
@click.option('--out_file', envvar='OUT_FILE', default = None, help="If given wirte into a file in the data directory")
def fillEventsFile(config, file, ignore_db, out_file):
    """
    Processes a file into the EventList db
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
    night = int(basename[:8])
    runId = int(basename[9:12])
    logger.debug("Basename: {}, Night: {}, runId: {}".format(basename, night, runId))
    df = None
    try:
        df = process_data_file(file)
    except:
        logger.error("Caught: "+str(type(e)))
        logger.error("###File: "+file+" ###\n")
        logger.error(str(e.args))
        logger.error("###end###\n")

    if df is None:
        logger.error("Couldn't process data file")
        return
    if out_file is None:
        logger.info("Update db")
        dbconfig  = config['processing_database']
        processing_db.init(**dbconfig)
        createTables()
        write_eventlist_into_database(file, night, runId, ignore_db, df)
    else:
        logger.info("Write data into file")
        output_folder = os.path.join(config['submitter']['data_directory'], "output")
        write_eventlist_into_file(file, night, runId, ignore_db, df, output_folder)

    logger.info("Finished Processing")    



import glob
@click.command()
@click.option(
    '--config', '-c', envvar='EVENTLIST_CONFIG',
    help='Config file, if not given, env EVENTLIST_CONFIG and ./eventlist.yaml will be tried'
)
@click.option('--ignore_db', is_flag=True, help="If given, ignore if the file is missing from the processing db and just add it")
@click.argument('datafolder', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True))
def updateEventListFromFile(config, ignore_db, datafolder):
    logger.info("Loading config")
    if not config:
        logger.error("No config specified, can't work without it")
        return
    config, configpath = load_config(config)

    dbconfig  = config['processing_database']
    processing_db.init(**dbconfig)
    createTables()
    
    
    files = glob.glob(datafolder+"/*.csv")
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
            logger.info("Set as error status and rename")
            info = ProcessingInfo.get((ProcessingInfo.night==night)&(ProcessingInfo.runId==runId))
            info.status=2
            info.save()
            os.rename(path, path+".dup")
            continue

        
        write_eventlist_into_database(basename, night, runId, ignore_db, df)
        
        logger.debug("Removing file")
        os.remove(path)
    
    logger.info("Finished inserting eventlist data from files")
    
