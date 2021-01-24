#!/usr/bin/python

import sys
import sqlite3

dbHistoryCon = None
dbHistoryCur = None
dbRatesCon = None
dbRatesCur = None

def openHistoryDB(dbFilename):
    global dbHistoryCon
    global dbHistoryCur

    try:
        dbHistoryCon = sqlite3.connect(dbFilename, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        dbHistoryCur = dbHistoryCon.cursor()
        return (dbHistoryCon, dbHistoryCur)

    except Exception as e:
        print("Error %s:" % str(e))
        sys.exit(1)


def openRatesDB(dbFilename):
    global dbRatesCon
    global dbRatesCur

    try:
        dbRatesCon = sqlite3.connect(dbFilename, detect_types=sqlite3.PARSE_DECLTYPES, check_same_thread=False)
        dbRatesCur = dbRatesCon.cursor()
        return (dbRatesCon, dbRatesCur)

    except Exception as e:
        print("Error %s:" % str(e))
        sys.exit(1)
