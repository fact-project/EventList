# EventList
This package contains tools to create a eventlist data base, which works as an index for all of FACTs events. Among other properties, its core purpose is to provide this index as their coordinates in the fact raw data structure i.a. night (YYYYMMDD), run_id, event_id, the event_type (e.g. data, pedestal, lightpulser).   

Furthermore, it provides tools to extract subsets of this databases as line based json files. The subset is created based on filters according to the events (to be exact the runs) properties in the RunINfo Database of FACT. With this allows you to get e.g. all pedestal events with currents above 6uA (non-dark night light conditions).

## Json subset extraction
There are two excecutables for this job: `noisedb` and `noisedb_condition`   

* `noisedb` - 
Gets all pedestal events coordinates (meaning: night, run, event_num, event_type, runtype) from the EventList database. Then it delivers a subset according to the provided conditions

* `noisedb_condition` - 
Gets pedestal events coordinates (meaning: night, run, event_num, event_type, runtype) from the EventList database according to the provided conditions.

# Installation
The whole package is pip installable. However, all non pypy repositories are listed in the requirements.txt. Install via:
```pip install -r requirements.txt```


