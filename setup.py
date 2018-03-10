import os
from setuptools import setup

# Utility function to read the README file.
# Used for the long_description.  It's nice, because now 1) we have a top level
# README file and 2) it's easier to type in the README file than to put a raw
# string in below ...
def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "eventlist",
    version = "0.0.1",
    author = "Michael Bulinski",
    author_email = "michael.bulinski@udo.edu",
    description = ("Functions to create and modify the eventlist database."),
    license = "GPL3",
    keywords = "fact database eventlist",
    url = "https://github.com/fact-project/eventlist",
    packages=['eventlist'],
    #long_description=read('README'),
    install_requires=[
        'peewee',
        'pyfact',
        'astropy',
        'click',
        'astropy',
        'zfits',
        'erna',
    ],
    entry_points={
        'console_scripts': [
            'eventListProcessFile = eventlist.scripts.eventListProcessFile:eventListProcessFile',
            'noisedb = eventlist.noiseDatabase:main',
            'noisedb_condition = eventlist.noiseDatabase:getNoiseDBcondition',
            'updateEventList = eventlist.database:processNewFiles',
            'fillEventListFromCSVFile = eventlist.database.scripts.updateEventListFromCSVFile:updateEventListFromCSVFile',
            'updateEventListFSStatus = eventlist.scripts.updateEventlistFSStatus:updateEventlistFSStatus'
        ],
    },
)
