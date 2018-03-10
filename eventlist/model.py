import peewee as pew
from enum import Enum

from playhouse.shortcuts import RetryOperationalError

class MyRetryDB(RetryOperationalError, pew.MySQLDatabase):
    pass

processing_db = MyRetryDB(None)

__all__ = ['processing_db_config', 'Event', 'ProcessStatus', 'ProcessingInfo', 'connect_processing_db']

processing_db_config = {
    "host" : "fact-mysql.app.tu-dortmund.de",
    "database" : "eventlist",
    "user" : "<user>",
    "password" : "<password>"
}


class Event(pew.Model):
    """
    Eventlist database model
    """
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
    """
    ProcessingInfo Database Model
    """
    night = pew.IntegerField()
    runId = pew.SmallIntegerField()
    extension = pew.CharField(6)
    status = pew.SmallIntegerField()
    # each supported filesystem has a field below
    isdc = pew.BooleanField(default=False)
    fhgfs = pew.BooleanField(default=False)
    bigtank = pew.BooleanField(default=False)
    
    class Meta:
        database = processing_db
        db_table = "File_Processing_Info"
        indexes = (
            (('night', 'runId'), True),
        )
    
    def getFileSystems():
        """
        Returns the supported filesystems
        """
        return ['isdc', 'fhgfs', 'bigtank']
    
    def isSupported(fs):
        """
        Checks if the given filesystem is supported
        """
        return fs in ProcessingInfo.getFileSystems()
    

def connect_processing_db(config):
    """
    Connect to the processing db and create the tables if they don't exist yet
    """
    processing_db.init(**config)
    processing_db.connect()
    processing_db.create_tables([Event, ProcessingInfo], safe=True)

