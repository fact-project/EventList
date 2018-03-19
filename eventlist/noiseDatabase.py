import click
import numpy as np

import pandas as pd

from fact.credentials import get_credentials
from fact.factdb import *
from fact.factdb.utils import read_into_dataframe

from .utils import load_config

import logging
import sys

logger = logging.getLogger('Noise_Databse')
logger.setLevel(logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

def getDrsFiles(night):
    """
    Get all the drs files for the given night and return as a dataframe
    """
    query = (
        RunInfo.select(
            RunInfo.fnight.alias("NIGHT"),
            RunInfo.frunid.alias("RUNID"),
            RunInfo.frunstart.alias("START"),
        )
        .where(RunInfo.fdrsstep == 2)
        .where(RunInfo.froi == 300)
        .where(RunInfo.fruntypekey == 2)
        .where(RunInfo.fnight == night)
    )
    
    df = read_into_dataframe(query)
    return df

def getClosestDrsFile(drsFiles, startTime):
    """
    Given a dataframe containing the drsfiles and a starttime calculate
    the closest two drs files to this startTime
    """
    delta = np.abs(drsFiles['START']-startTime)
    # calculate closest
    minIndex = np.argmin(delta)
    # calculate second closest
    if minIndex == len(drsFiles)-1:
        secMinIndex = minIndex-1
    elif minIndex == 0:
        secMinIndex = 1
    else:
        secMinIndex = minIndex-1 if delta[minIndex-1]<delta[minIndex+1] else minIndex+1
    
    return [drsFiles['RUNID'][minIndex], drsFiles['RUNID'][secMinIndex]]
    
'fNight <= 20140203 AND fNight >= 20131001 AND fSourceName = "Crab" AND fCurrentsMedMeanBeg < 8 AND fZenithDistanceMax < 30 AND fMoonZenithDistance > 100 AND fThresholdMinSet < 350 AND fEffectiveOn > 0.95 AND fTriggerRateMedian > 40 AND fTriggerRateMedian < 85 AND fThresholdMinSet < (14 * fCurrentsMedMeanBeg + 265)'

def getRunInfos(night, runid, condition=None):
    """
    Get the RunInfo for the given night and runid, if a query is given test it and if it fails return None to discard run
    """
    query = (
        RunInfo.select(
            RunInfo.fnight,
            RunInfo.frunid,
            RunInfo.fcurrentsmedmean,
            RunInfo.fcurrentsmedmeanbeg,
            RunInfo.fzenithdistancemean,
            RunInfo.fzenithdistancemax,
            RunInfo.fmoonzenithdistance,
            RunInfo.fthresholdminset,
            RunInfo.feffectiveon,
            RunInfo.ftriggerratemedian,
            Source.fsourcename,
        )
        .join(Source, on=(Source.fsourcekey == RunInfo.fsourcekey))
        .where(RunInfo.fnight == night)
        .where(RunInfo.frunid == runid)
    )
    
    df = read_into_dataframe(query)
    #print(df)
    if len(df) == 0:
        raise LookupError("Missing run infos")
        
    if condition is not None:
        print(condition)
        df.query(condition)
    
    if len(df)==0:
        return None

    current = df.fCurrentsMedMean.values[0]
    zdDistMean = df.fZenithDistanceMean.values[0]
    #zdDistMax = df.fZenithDistanceMax.values[0]
    source = df.fSourceName.values[0]
    moonZdDist = df.fMoonZenithDistance.values[0]
    return [current, zdDistMean, source, moonZdDist]

from datetime import datetime
from .model import processing_db_config, Event, connect_processing_db, ProcessingInfo

@click.command()
@click.option(
    '--config', '-c', envvar='EVENTLIST_CONFIG',
    help='Config file, if not given, env EVENTLIST_CONFIG and ./eventlist.yaml will be tried'
)
@click.option('--datacheck', '-d', help='The datacheck to use on the database')
@click.option('--firstnight', '-f', type=int, help='First night to consider')
@click.option('--lastnight', '-l', type=int, help='Last night to consider')
@click.option('--condition', '-c', help='Only use events that fullfill this condition')
@click.option('--limitevents', default=1000, help='It is not possible to process all events at once so we pull a limit of this every time')
@click.argument('outdb', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
def main(outdb, config, datacheck, firstnight, lastnight, condition, limitevents):
    """
    Create the noisedb from the EventListDB
    """
    creds = get_credentials()
    password = dict(creds['sandbox'])['password']
    processing_db_config["user"] = 'fact'
    processing_db_config["password"] = password

    logger.info("Connecting to DB")
    connect_processing_db(processing_db_config)
    
    # get all Events that are pedestal Events (pedestals, interleave pedestal and gps trigger)
    logger.info("Create query")
    query = Event.select().where((Event.eventType == 1024) | (Event.eventType == 1)).limit(limitevents)
    if firstnight is not None:
        logger.info("First night to consider: {}".format(firstnight))
        query = query.where(Event.night>=firstnight)
    if lastnight is not None:
        logger.info("Last night to consider: {}".format(lastnight))
        query = query.where(Event.night<=lastnight)
    logger.info("Created Query")
    logger.debug(query)
    
    curNight = 0
    drsFiles = None
    curRunId = 0
    closestDrsFiles = None
    runInfos = None
    
    noiseData = []
    offset = 0
    print("Process events")
    hadEvents = True
    while query.iterator():
        print("Process form offset: {}".format(offset))
        query = query.offset(offset)
        for d in query.iterator():
            hadEvents = True
            # check if the night changed if yes load the drs files for that night
            night = d.night
            if night != curNight:
                print("New night to process: "+str(night))
                curNight = night
                drsFiles = getDrsFiles(night)
                print("Drs Files for current night:")
                print(drsFiles)
            runId = d.runId
            if curRunId != runId:
                print("New runId to process: "+str(runId))
                curRunId = runId
                startTime = np.datetime64(datetime.utcfromtimestamp(d.UTC))
                #print(startTime)
                closestDrsFiles = getClosestDrsFile(drsFiles, startTime)
                print("Drs files for current run: {}".format(closestDrsFiles))
                runInfos = getRunInfos(night, runId, condition)
                print("Runinfos:")
                print(runInfos)
            if runInfos is None:
                continue

            #print(d.eventNr)
            res = [d.eventNr, d.UTC, d.night, d.runId, closestDrsFiles[0], closestDrsFiles[1]] + runInfos
            noiseData.append(res)
        if not hadEvents:
            break
        hadEvents = False
        offset += limitevents
    
    df_temp = pd.DataFrame(noiseData, columns=
                ['eventNr', 'UTC','NIGHT','RUNID', 'drs0', 'drs1',
                 'currents', 'Zd', 'source','moonZdDist'])
    df_temp.to_json(outdb, orient='records', lines=True)


from .conditions import conditions
@click.command()
@click.option(
    '--config', '-c', envvar='EVENTLIST_CONFIG',
    help='Config file, if not given, env EVENTLIST_CONFIG and ./eventlist.yaml will be tried'
)
@click.option('--datacheck', '-d', help='The datacheck to use on the database')
@click.option('--firstnight', '-f', type=int, help='First night to consider')
@click.option('--lastnight', '-l', type=int, help='Last night to consider')
@click.option('--condition', help='Only use events that fullfill this condition type')
@click.option('--fs', default='isdc', type=click.Choice(ProcessingInfo.getFileSystems()), help='Which filesystem to use: [isdc,fhgfs,bigtank]')
@click.option('--source', help='Which source should be choosen')
@click.argument('outdb', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
def getNoiseDBcondition(outdb, config, datacheck, firstnight, lastnight, condition, source, fs):
    """
    Create the noisedb from the EventListDB given a set of conditions to the used runs
    """
    logger.info("Loading config")
    if not config:
        logger.error("No config specified, can't work without it")
        return
    config, configpath = load_config(config)

    logger.debug("Connect to processing database")
    dbconfig  = config['processing_database']
    connect_processing_db(dbconfig)
    
    logger.debug("Connect to fact database")
    fact_db_config = config['fact_database']
    connect_database(fact_db_config)
    
    # get all usable files
    query = (RunInfo.select(RunInfo.fnight.alias('night'),
                           RunInfo.frunid.alias('runId'))
        .join(Source, on=(Source.fsourcekey == RunInfo.fsourcekey))
    )
    if firstnight is not None:
        logger.debug("Add condition for first night: {}".format(firstnight))
        query = query.where(RunInfo.fnight >= firstnight)
    if lastnight is not None:
        logger.debug("Add condition for last night: {}".format(lastnight))
        query = query.where(RunInfo.fnight <= lastnight)
    
    if condition is not None:
        cond = conditions[condition]
        for c in cond:
            query = query.where(c)
    if source is not None:
        logger.debug("Add condition for source: {}".format(source))
        query = query.where(Source.fsourcename==source)

    df_runinfo = pd.DataFrame(list(query.dicts()))
    
    logger.info("Amout of admissable Runs: {}".format(len(df_runinfo)))
    
    # get all processed files
    query = ProcessingInfo.select(ProcessingInfo.night, ProcessingInfo.runId).where(ProcessingInfo.status==1)
    if firstnight is not None:
        query = query.where(ProcessingInfo.night >= firstnight)
    if lastnight is not None:
        query = query.where(ProcessingInfo.night <= lastnight)
    
    # get all files that are still on the fiven filesystem
    query = query.where(getattr(ProcessingInfo, fs) == True)
    
    df_processinginfo = pd.DataFrame(list(query.dicts()))
    logger.info("Possible processed runs: {}".format(len(df_processinginfo)))
    df_processedruns = df_processinginfo.merge(df_runinfo, on=['night','runId'])
    logger.info("Possible processed runs with condition: {}".format(len(df_processedruns)))
    
    
    curNight = 0
    drsFiles = None
    curRunId = 0
    closestDrsFiles = None
    runInfos = None
    
    noiseData = []
    logger.info("Process events")
    for index, row in df_processedruns.iterrows():
        logger.info("Processing Run: {}_{}".format(row['night'],row['runId']))
        query = ( Event.select()
                .where(Event.night==row['night'])
                .where(Event.runId==row['runId'])
                .where((Event.eventType == 1024) | (Event.eventType == 1))
        )
        for d in query.iterator():
            # check if the night changed if yes load the drs files for that night
            night = d.night
            if night != curNight:
                logger.info("New night to process: "+str(night))
                curNight = night
                drsFiles = getDrsFiles(night)
                logger.info("Drs Files for current night:")
                logger.info(drsFiles)
                curRunId = -1 # the night changed so the curRunId changed too
            # check if the run changed if yes recalculate the closest drs file
            runId = d.runId
            if curRunId != runId:
                logger.info("New runId to process: "+str(runId))
                curRunId = runId
                startTime = np.datetime64(datetime.utcfromtimestamp(d.UTC))
                #print(startTime)
                closestDrsFiles = getClosestDrsFile(drsFiles, startTime)
                logger.info("Drs files for current run: {}".format(closestDrsFiles))
                runInfos = getRunInfos(night, runId)
                logger.info("Runinfos:")
                logger.info(runInfos)
            # infos about the runs are missing ignore
            if runInfos is None:
                logger.error("Missing run infos for run: {}_{}".format([night,  runId]))
                continue

            #print(d.eventNr)
            res = [d.eventNr, d.UTC, d.night, d.runId, closestDrsFiles[0], closestDrsFiles[1]] + runInfos
            noiseData.append(res)
    
    logger.info("Finished fetching all events. Creating database.")
    
    df_temp = pd.DataFrame(noiseData, columns=
                ['eventNr', 'UTC','NIGHT','RUNID', 'drs0', 'drs1',
                 'currents', 'Zd', 'source','moonZdDist'])
    df_temp.to_json(outdb, orient='records', lines=True)
    logger.info("Finished")
