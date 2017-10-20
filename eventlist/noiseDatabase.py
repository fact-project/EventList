import peewee as pew
import click
from glob import glob
import os
import numpy as np

import pandas as pd

from fact.credentials import create_factdb_engine, get_credentials
from fact.factdb import *
from fact.factdb.utils import read_into_dataframe

def getDrsFiles(night):
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

def getRunInfos(night, runid):
    query = (
        RunInfo.select(
            RunInfo.fnight,
            RunInfo.frunid,
            RunInfo.fcurrentsmedmean,
            RunInfo.fzenithdistancemean,
            RunInfo.fmoonzenithdistance,
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

    current = df.fCurrentsMedMean.values[0]
    zd = df.fZenithDistanceMean.values[0]
    source = df.fSourceName.values[0]
    moonZdDist = df.fMoonZenithDistance.values[0]
    return [current, zd, source, moonZdDist]

from datetime import datetime
from .database import dbconfig, Event, db

@click.command()
@click.argument('outdb', type=click.Path(exists=False, dir_okay=False, file_okay=True, readable=True) )
def main(outdb):
    creds = get_credentials()
    password = dict(creds['sandbox'])['password']
    dbconfig["password"] = password
    db.init(**dbconfig)
    db.connect()
    
    #data = Event.select().where((Event.eventType == 1024) | (Event.eventType == 1))
    data = Event.select().where((Event.eventType == 1) | (Event.eventType == 4))
    
    curNight = 0
    drsFiles = None
    curRunId = 0
    closestDrsFiles = None
    runInfos = None
    
    noiseData = []
    for d in data:
        # check if the night changed if yes load the drs files for that night
        night = d.night
        if night != curNight:
            print("New night to process: "+str(night))
            curNight = night
            drsFiles = getDrsFiles(night)
        runId = d.runId
        if curRunId != runId:
            print("New runId to process: "+str(runId))
            curRunId = runId
            startTime = np.datetime64(datetime.utcfromtimestamp(d.UTC))
            #print(startTime)
            closestDrsFiles = getClosestDrsFile(drsFiles, startTime)
            print(closestDrsFiles)
            runInfos = getRunInfos(night, runId)
            print(runInfos)
            
        #print(d.eventNr)
        res = [d.eventNr, d.UTC, d.night, d.runId, closestDrsFiles[0], closestDrsFiles[1]]+runInfos
        noiseData.append(res)
    
    df_temp = pd.DataFrame(noiseData, columns=
                ['eventNr', 'UTC','NIGHT','RUNID', 'drs0', 'drs1',
                 'currents', 'Zd', 'source','moonZdDist'])
    df_temp.to_json(outdb, orient='records', lines=True)
