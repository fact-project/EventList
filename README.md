# EventList
This package contains tools to create an eventlist data base, which works as an index for all of FACTs events. Among other properties, its core purpose is to provide this index as their coordinates in the fact raw data structure i.a. night (YYYYMMDD), run_id, event_id, the event_type (e.g. data, pedestal, lightpulser).   

Furthermore, it provides tools to extract subsets of this databases as line based json files. The subset is created based on filters according to the events (to be exact the runs) properties in the RunINfo Database of FACT. With this allows you to get e.g. all pedestal events with currents above 6uA (non-dark night light conditions).

## JSON subset extraction
To extract the json noise database there is one function for it.

* `el_create_noise_db` - 
Gets all pedestal events coordinates (meaning: night, run, event_num, event_type, runtype) from the EventList database. Then it delivers a subset according to the provided conditions.
It also calculates the two closest drs files for each event.

## Index Generation
For the index generation there are three executables described below, although only `el_update_index` and `el_fill_index_from_csv` need to be interacted with directly.

* `el_update_index`
Main executable to generate the eventlist index. The executable does 2 things. First it updates the processing database with all new created files from La Palma. Second it processes all files currently not part of the index and availibly to the machine.
To create the index this executable calls `el_generate_index` for each file to generate.

* `el_generate_index`
Given a data file creates the index for the given file and either updates the eventlist database or creates a csv file with the information.

* `el_fill_index_from_csv`
Fills the eventlist index with event information given from csv files generated with `el_generate_index`. Manly used on the isdc due to the fact that there is no direct connection to the eventlist db from the processing machines.

## Processing Database Functions
Given the fluctuating nature of the availibility of our files the following execuatable allows to update the availibility columns in the processing db.

* `el_update_processing_db_fs_status`
Updates for a given filesystem the current availibility of the files that are still existing.

# Installation
The whole package is pip installable. However, all non pypy repositories are listed in the requirements.txt. Install via:
```pip install -r requirements.txt```


