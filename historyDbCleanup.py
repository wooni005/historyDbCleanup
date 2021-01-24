#!/usr/bin/python3

#Bron: http://nbviewer.jupyter.org/github/home-assistant/home-assistant-notebooks/blob/master/graph-single-sensor.ipynb
import subprocess
import time

#De libs met functies per soort tabel
import verbruikWater
import verbruikElektra
import metingTemp
import weerStation
import barometer
import upsStatus

import db

DB_DIR  = "/home/pi/db"
#DB_DIR  = "/home/pi/scripts/python/historyDbCleanup"
DB_HISTORY_FILE = "history.db"
DB_DOMA_FILE    = "doma.db"
DB_RATE_FILE    = "rates.db"

dbHistoryTabels = ['verbruikWater', 'verbruikElektra', 'temperatuur', 'weerStation', 'barometer', 'upsStatus']
#dbHistoryTabels = ['barometer']
N_DAGEN_NIET_CLEANEN = 1 #1=Laat vandaag en gisteren met rust (24 tot 48 uur)
N_DAGEN_CLEANEN = 30
nDagenTerug = N_DAGEN_NIET_CLEANEN

#################################
# Open de historieDB
#################################
timeStr = time.strftime("%Y-%m-%d_%H.%M.%S")
subprocess.call("cd %s ;sqlite3 %s '.backup backup/%s-history.db'" % (DB_DIR, DB_HISTORY_FILE, timeStr), shell=True)
subprocess.call("cd %s ;sqlite3 %s '.backup backup/%s-doma.db'" % (DB_DIR, DB_DOMA_FILE, timeStr), shell=True)

#################################
# Open de historieDB
#################################
db.openHistoryDB(DB_DIR+'/'+DB_HISTORY_FILE)
db.openRatesDB(DB_DIR+'/'+DB_RATE_FILE)

#################################
# Zet alle totalen in de historieDB
#################################
for table in dbHistoryTabels:
    exit = False
    dbError = False
    nDagenTerug = N_DAGEN_NIET_CLEANEN
    while not exit:
        nDagenTerug += 1 # Verder een dag terug

        if table == 'verbruikWater':
            end, dbError = verbruikWater.insertTotalsPerDay(nDagenTerug, table)
        elif table == 'verbruikElektra':
            end, dbError = verbruikElektra.insertTotalsPerDay(nDagenTerug, table)
        elif table == 'temperatuur':
            end, dbError = metingTemp.insertTotalsPerDay(nDagenTerug, table)
        elif table == 'weerStation':
            end, dbError = weerStation.insertTotalsPerDay(nDagenTerug, table)
        elif table == 'barometer':
            end, dbError = barometer.insertTotalsPerDay(nDagenTerug, table)
        elif table == 'upsStatus':
            end, dbError = upsStatus.insertTotalsPerDay(nDagenTerug, table)
        else:
            print(('INSERT: Onbekende tabel naam: %s' % table))

        #if nDagenTerug > N_DAGEN_CLEANEN: #Forceer een DB cleanup actie tot N_DAGEN_CLEANEN dagen terug (wanneer een sensor een tijdje is uitgevallen geweest.
        if end or dbError:
            #Stop while loop bij dbError
            break

    if dbError:
        #Stop for loop bij dbError
        break

####################################################
# Verwijder de metingen van vorige dagen uit de historyDB
####################################################
for table in dbHistoryTabels:
    if table == 'verbruikWater':
        dbError = verbruikWater.deleteAllOldMeasurements(table, N_DAGEN_NIET_CLEANEN, nDagenTerug)
    elif table == 'verbruikElektra':
        dbError = verbruikElektra.deleteAllOldMeasurements(table, N_DAGEN_NIET_CLEANEN, nDagenTerug)
    elif table == 'temperatuur':
        dbError = metingTemp.deleteAllOldMeasurements(table, N_DAGEN_NIET_CLEANEN, nDagenTerug)
    elif table == 'weerStation':
        dbError = weerStation.deleteAllOldMeasurements(table, N_DAGEN_NIET_CLEANEN, nDagenTerug)
    elif table == 'barometer':
        dbError = barometer.deleteAllOldMeasurements(table, N_DAGEN_NIET_CLEANEN, nDagenTerug)
    elif table == 'upsStatus':
        dbError = upsStatus.deleteAllOldMeasurements(table, N_DAGEN_NIET_CLEANEN, nDagenTerug)
    else:
        print(('DELETE: Onbekende tabel naam: %s' % table))

db.dbHistoryCon.commit()
db.dbHistoryCon.execute("VACUUM")
db.dbHistoryCon.close()

db.dbRatesCon.commit()
db.dbRatesCon.execute("VACUUM")
db.dbRatesCon.close()
