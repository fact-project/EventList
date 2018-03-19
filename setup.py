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
        # 'zfits', #needs to be installed with the requirements.txt
        # 'erna', #needs to be installed with the requirements.txt
    ],
    entry_points={
        'console_scripts': [
            'el_generate_index_from_file = eventlist.scripts.eventListProcessFile:eventListProcessFile',
            'el_create_noise_db = eventlist.noiseDatabase:main',
            'el_create_noise_db_condition = eventlist.noiseDatabase:getNoiseDBcondition',
            'el_update_index = eventlist.database:processNewFiles',
            'el_fill_index_from_csv = eventlist.database.scripts.updateEventListFromCSVFile:updateEventListFromCSVFile',
            'el_update_processing_db_fs_status = eventlist.scripts.updateEventlistFSStatus:updateEventlistFSStatus'
        ],
    },
)
