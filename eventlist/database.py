import peewee as pew
import click
from glob import glob
import os
import numpy as np
import pandas as pd
from fact.credentials import get_credentials

import logging
import time

from enum import Enum

from playhouse.shortcuts import RetryOperationalError


logger = logging.getLogger('EventList')
logger.setLEvel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


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


class ProcessStatus(Enum):
    not_processed = 0
    processed = 1
    error = 2

class FileType(enum):
    unknown = 0
    fz = 1
    gz = 2
    
class ProcessingInfo(pew.Model):
    night = pew.IntegerField()
    runId = pew.SmallIntegerField()
    fileType = pew.SmallIntegerField()
    status = pew.SmallIntegerField()
    isdc = pew.BooleanFiled()
    
    class Meta:
        database = db
        db_table = "File_Processing_Info"
        indexes = (
            (('night', 'runId'), True),
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
    db.create_tables([Event, Files], safe=True)


def process_data_file(filename):
    ext = os.path.splitext(filename)[1]
    basename = os.path.basename(filename)
    df = None
    if ext == ".gz":
        if filename[-12:] == ".drs.fits.gz":
            logger.info("Drs File Skipping")
            return
        df = processFitsFile(filename)
    elif ext == ".fz":
        df = processZFitsFile(filename)
    else:
        logger.error("Unknown extension: '"+ext+"' of file: '"+filename+"', skipping")
        return None
    
def process_file(filename, outfolder=None):
    ext = os.path.splitext(filename)[1]
    basename = os.path.basename(filename)
    df = process_data_file(filename)

    if not df:
        return
    
    if outfolder:
        outfile = outfolder+"/output-"+basename+"-.csv"
        print("  Write data into file: "+outfile)
        with open(outfile, "w") as out:
            df.to_csv(out, index=False)
    else:
        print("  Insert data into DB")
        with db.atomic():
            Event.insert_many(**(df.to_dict(orient='records'))).execute()

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


def getAllNewFiles():
    """
    Returns all files that are currently not part of the ProcessingInfo db
    """
    query = (
        RunInfo.select(
            RunInfo.fnight.alias('night'),
            RunInfo.frunid.alias('runid'),
        )
        .where(RunInfo.not_in(ProcessingInfo))
        .where(RunInfo.froi == 300)
        .where((RunInfo.fruntypekey == 2)|(RunInfo.fruntypekey == 1))
        .where(RunInfo.fdrsstep.is_null(True))
    )
    
    df = read_into_dataframe(query)
    return df
    
def getAllNotProcessedFiles():
    """
    Return all files that have yet to be processedm, on the isdc
    """
    query = (
        ProcessingInfo.select(
            ProcessingInfo.night,
            ProcessingInfo.runId,
        )
        .where(ProcessingInto.processed == 0)
        .where(ProcessingInto.isdc == True)
    )
    
    df = read_into_dataframe(query)
    return df

def returnPathIfExists(rawfolder, night, runId):
    """
    Creates a full path for the specific run and test wheater it is an fz or gz file and if it exists
    """
    year = night//10000
    month = (night%10000)//100
    day = night%100
    
    path = os.path.join(rawfolder, "{:04d}/{:02d}/{:02d}/{:08d}_{:03d}.fits".format(year,month,day,night,runId)
    
    if os.path.exists(path+".fz"):
        return path+".fz"
    if os.path.exists(path+".gz"):
        return path+".gz"
    return None

from erna.automatic_processing.qsub import ( get_current_jobs, build_qsub_command)

def getAllRunningFiles(jobs):
    """
    Get all files that are still running or are currently pending
    """
    jobs[jobs.JB_name.str.startswith('eventlist_')]
    files = jobs.JB_name.str[10:]
    night = jobs.JB_name.str[10:18].astype(int)
    runId = jobs.JB_name.str[19:22].astype(int)
    
    df = pd.DataFrame({'files':files, 'night':night, 'runId':runId)
    return df


def createQsub(file, log_dir, env, kwargs):
    """
    Creates a new qsub to process a single file into the eventlist database
    """
    
    basename = os.path.basename(file)
    
    executable = sp.check_output(
        ['which', 'eventlistfile']
    ).decode().strip()
    
    
    command = build_qsub_command(
        executable=executable,
        job_name="eventlist_"+basename),
        environment=env,
        stdout = os.path.join(log_dir, 'eventlist_{}.o'.format(basename)),
        stderr = os.path.join(log_dir, 'eventlist_{}.e'.format(basename)),
        **kwargs,
    )

    return command
    

from erna import utils

@click.command()
@click.argument('rawfolder', type=click.Path(exists=True, dir_okay=True, file_okay=False, readable=True))
@click.argument('logdir', type=click.Path(exists=False, dir_okey=True, file_okey=False, readable=True))
@click.option('--queue', default='short')
@click.option('--walltime', default='00:10:00')
@click.option('--password', default=None)
@click.option(
    '--config', '-c',
    help='Config file, if not given, env ERNA_CONFIG and ./erna.yaml will be tried'
)
@click.option(
    '--verbose', '-v', help='Set log level of "erna" to debug', is_flag=True,
)
def processNewFiles(rawfolder, logdir, queue, walltime, password, config):
    """
    Processes a file into the EventList db
    """
        
    if verbose:
        logging.getLogger('EventList').setLevel(logging.DEBUG)
    else:
        logging.getLogger('EventList').setLevel(logging.INFO)

    logging.info("Load Config")
    config = utils.load_config(config)
    interval = config['submitter']['interval'],
    max_queued_jobs = config['submitter']['max_queued_jobs'],
    os.makedirs(logdir, exist_ok=True)

    if not password:
        creds = get_credentials()
        password = dict(creds['sandbox'])['password']
    else:
        password = password
    dbconfig["password"] = password
    db.init(**dbconfig)
    
    createTables()

    logger.info("Processing all new files and yet not processed files into the db")
    logger.info("Update the Processing db")
    df = getAllNewFiles()
    newFiles = []
    for d in df:
        night = d['night']
        runId = d['runId']
        path = returnPathIfExists(night, runId)
        ext = os.path.splitext(path)[1][1:]
        if not path:
            # New file but missing on the isdc
            newFiles.append({'night':night, 'runId'=runId, 'fileType'=0, status=0, isdc=True})
        else:
            newFiles.append({'night':night, 'runId'=runId, 'fileType'=FileType[ext].value, status=0, isdc=True})
    logger.info("Insert all new Files")
    with db.atomic():
        Event.insert_many(newFiles).execute()
    
    logger.info("Get all unprocessed files")
    df = getAllNotProcessedFiles()
    
    qsub_env = {
        "WALLTIME": walltime,
        "FACT_PASSWORD": password,
    }
    
    qsub_kwargs = {
        'mail_address' : config['submitter']['mail_address'],
        'mail_settings' : config['submitter']['mail_settings'],
        'queue' : queue,
    }

    logger.info("Process all unprocessed files")
    try:
        for d in df:
            night = d['night']
            runId = d['runId']
            
            ext = FileType(d['fileType']).name
            year = night//10000
            month = (night%10000)//100
            day = night%100
        
            path = os.path.join(rawfolder, "{:04d}/{:02d}/{:02d}/{:08d}_{:03d}.fits.{}".format(year, month, day, night, runID, ext)
            logger.info("Processing night: {}, runId{}".format(nigth, runId))
            logger.info("  Path: "+path);
            
            logger.info("Get all still running or pending files")
            current_jobs = get_current_jobs()
            runningFiles = getAllRunningFiles()
            
            if ((runningFiles['night'] == night) & (runningFiles['runId'] == runId)).any():
                logger.info("File already in processing skipping")
                continue
            
            # create qsub command
            cmd = create_qsub(path, logdir, qsub_env, qsub_kwargs)
            
            # execute
            while True:
                if len(queued_jobs) < max_queued_jobs:
                    logger.info("Sending to qsub")
                    output = sp.check_output(cmd)
                    logger.debug(output.decode().strip())
                    break
                time.sleep(interval)
            time.sleep(interval)
            
    except (KeyboardInterrupt, SystemExit):
        logger.info('Shutting done')
        log.info('Clean up running jobs')
        current_jobs = get_current_jobs()
        myjobs = jobs[jobs.JB_name.str.startswith('eventlist_')]
        for job in myjobs
            sp.run(['qdel', job['JB_name']])

        
        
    

@click.command()
@click.option('--file', default=None, type=click.Path(exists=True, dir_okay=False, file_okay=True, readable=True))
@click.option('--password', default=None)
@click.option('--ignore_db', is_flag=True, help="If given, ignore if the file is missing from the processing db and just add it")
def fillEventsFile(file, password, ignore_db)
    """
    Processes a file into the EventList db
    """
    if not password:
        creds = get_credentials()
        password = dict(creds['sandbox'])['password']
    else:
        password = password
    dbconfig["password"] = password
    db.init(**dbconfig)
    
    createTables()
    
    logger.info("Processing file: '"+file+"'")
    
    if not file:
        file = os.environ['FILE']

    if not os.path.exists(file):
        logger.error("File does not exists")
        return

    basename = os.path.basename(file)
    night = int(basename[:8])
    runId = int(basename[9:11])
    df = None
    try:
        df = process_data_file(file)
    except:
        logger.error("Caught: "+str(type(e))
        logger.error("###File: "+file+" ###\n")
        logger.error(str(e.args))
        logger.error("###end###\n")

    if not df:
        logger.error("Couldn't process data file")
    logger.info("Update db")
    with db.atomic():
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
                fileInfo = ProcessingInfo.create(night=night, runId=runId, fileType=FileType[ext].value, status=0, isdc=True)

        if fileInfo.status==1:
            logger.error("File is already processed, have you started the processing twice on this file?")
            return
        
        logger.debug("Insert Data")
        Event.insert_many(**(df.to_dict(orient='records'))).execute()
        
        logger.debug("Update processing db")
        fileInfo.processed = 1
        fileInfo.save()

    