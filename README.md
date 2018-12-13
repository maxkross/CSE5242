# CSE5242
PostgreSQL to Pythia .conf query conversion

This python program is compatible with versions of Python >= 3.6.

SETUP

1. Install required dependencies using `pip3.6 install -r requirements.txt`

EXECUTE

1. The main.py expects filepath as it's argument. Invoke the program as `python main.py queries\select.sql

Sample Pythia tables are in the /data/ directory as .tbl files

The corresponding sample PostgreSQL tables are in /data/ as pg_pythia_bkp.sql

The tables can be imported into a postgres database using the guide found on this page: https://www.postgresql.org/docs/9.1/backup-dump.html
