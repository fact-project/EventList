import peewee as pew
import click
import os
import subprocess as sp
import pandas as pd
from fact.factdb import (RunInfo, RawFileAvailISDCStatus, connect_database)

from .utils import load_config

import logging
import time

from eventlist.model import *


logger = logging.getLogger('EventList')
logger.setLevel(logging.DEBUG)
#import sys
#logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))



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
    
def getAllNotProcessedFiles(filesystem='isdc'):
    """
    Return all files that have yet to be processed and are existend on the choosen filesystem

    Parameters:
    filesystem -- The filesystem to use
    """
    query = (
        ProcessingInfo.select(
            ProcessingInfo.night,
            ProcessingInfo.runId,
            ProcessingInfo.extension,
        )
        .where(ProcessingInfo.status == 0)
        .where(getattr(ProcessingInfo, filesystem) == True)
    )
    
    df = pd.DataFrame(list(query.dicts()), columns=["night", "runId", "extension"])
    return df

from fact.path import tree_path

def returnPathIfExists(rawfolder, night, runId):
    """
    Creates a full path for the specific run and test wheater it is an fz or gz file and if it exists
    """
    path = tree_path(night, runId, rawfolder, ".fits")
    
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


def add_new_files(limit, rawfolder, fs):
    """
    Checks for new files and adds them to the processing db
    
    @limit only add this amount of new files
    @fs the filesystem to use
    """
    logger.debug("Getting all new files")
    df = getAllNewFiles(limit)
    logger.info("Found: {} new files start processing".format(len(df)))
    
    # add all new files into the processing db
    if len(df) != 0:
        newFiles = []
        logger.debug("Prepare the new files for the database")
        for index, row in df.iterrows():
            night = row['night']
            runId = row['runId']
            path = returnPathIfExists(rawfolder, night, runId)
            if not path:
                # New file but missing on the filesystem
                newFiles.append({'night':night, 'runId':runId, 'extension':"", 'status':0, fs:False})
            else:
                ext = os.path.splitext(path)[1][1:]
                newFiles.append({'night':night, 'runId':runId, 'extension':ext, 'status':0, fs:True})
        logger.info("Insert all new Files")
        with processing_db.atomic():
            ProcessingInfo.insert_many(newFiles).execute()
    else:
        logger.info("No new files for the processing database")
    logger.info("Added new files")

from .qsub import create_qsub,  get_current_jobs

def nightToDate(night):
    year = night//10000
    month = (night%10000)//100
    day = night%100
    return year,  month,  day
    
@click.command()
@click.argument('rawfolder', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True))
@click.option(
    '--config', '-c', envvar='EVENTLIST_CONFIG',
    help='Config file, if not given, env EVENTLIST_CONFIG and ./eventlist.yaml will be tried'
)
@click.option(
    '--verbose', '-v', help='Set log level of "erna" to debug', is_flag=True,
)
@click.option('--fs', default='isdc', type=click.Choice(ProcessingInfo.getFileSystems()), help='Which filesystem to use')
@click.option('--no_process', is_flag=True, help='Only fill in the processing database')
@click.option('--ignore_new',  is_flag=True, help='Do not check for new files')
@click.option('--usefile',  is_flag=True,  help='Specifies wheater to create a csv file instead of adding the events into the eventlist. ISDC needs this.')
@click.option('--limit_new', type=int, default=None,
    help='specify if the amount of new files should be limited and by how much.'
)
@click.option('--limit_process', type=int, default=None,
    help='specify if the amount of files to process should be limited and by how much.'
)
def processNewFiles(rawfolder, no_process, config, limit_new,  limit_process, verbose,  ignore_new,  fs,  usefile):
    """
    Processes all non processed files into the EventList db
    
    @ignore_new If not given will not check for new files currently not in the processing db and just
        process the unprocessed ones
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

    #get the configuration for the cluster job
    interval = config['submitter']['interval']
    max_queued_jobs = config['submitter']['max_queued_jobs']
    log_dir  = os.path.join(config['submitter']['data_directory'], "logs")
    queue    = config['submitter']['queue']
    walltime = config['submitter']['walltime']
    
    logger.debug("Configuration data:")
    logger.debug("Interval: {}, queue: {}, max queued jobs: {}".format(interval, queue, max_queued_jobs))
    logger.debug("walltime: {}, log dir: {}, usefile: {}".format(walltime, log_dir,  usefile))
    #os.makedirs(log_dir, exist_ok=True)

    logger.info("Connect to the databases")
    logger.debug("Connect to processing database")
    dbconfig = config['processing_database']
    connect_processing_db(dbconfig)
    
    logger.debug("Connect to fact database")
    fact_db_config = config['fact_database']
    connect_database(fact_db_config)
    
    if not ignore_new:
        add_new_files(limit_new, rawfolder, fs)

    if no_process:
        logger.info("Not processing files")
        logger.info("Finished")
        return

    logger.info("Get all unprocessed files")
    df = getAllNotProcessedFiles(fs)
    logger.info("Found: {} unporcessed files, start processing".format(len(df)))
    
    qsub_env = {
        "WALLTIME": walltime,
        'EVENTLIST_CONFIG': configpath,
        'OUT_FILE': str(usefile)
    }
    
    qsub_kwargs = {
        'mail_address' : config['submitter']['mail_address'],
        'mail_settings' : config['submitter']['mail_settings'],
        'queue' : queue,
    }

    logger.info("Process all unprocessed files")
    if limit_process is not None:
        logger.info("Processing maximum of {} files".format(limit_process))
    try:
        for index, row in df.iterrows():
            if limit_process is not None:
                if index==limit_process:
                    logger.info("Reached allowed limit of files to process")
                    break
            
            night = row['night']
            runId = row['runId']
            
            ext = row['extension']
            year,  month, day = nightToDate(night)
        
            path = os.path.join(rawfolder, "{:04d}/{:02d}/{:02d}/{:08d}_{:03d}.fits.{}".format(year, month, day, night, runId, ext))
            logger.info("Processing night: {}, runId:{}".format(night, runId))
            logger.info("  Path: "+path);
            
            logger.debug("Get all still running or pending files")
            current_jobs = get_current_jobs()
            runningFiles = getAllRunningFiles(current_jobs)
            
            # TODO check for finished files here also
            if ((runningFiles['night'] == night) & (runningFiles['runId'] == runId)).any():
                logger.info("  File already in processing skipping")
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
                fileInfo = ProcessingInfo.create(night=night, runId=runId, extension=ext, status=0, isdc=False, fhgfs=False,  bigtank=False)

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

