import db

def getValuesPerDay(daysBack, table):
    db.dbHistoryCur.execute("SELECT timestamp, batteryVoltage, lineVoltage, batteryCharge, upsLoad FROM %s WHERE timestamp >= date('now', '-%d day') AND timestamp < date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0')) ORDER BY timestamp" % (table, daysBack , (daysBack-1)))
    return db.dbHistoryCur.fetchall()

def getTotalsPerDay(daysBack, table):
    usageTable = getValuesPerDay(daysBack, table)
    
    sumLineVoltage = 0
    nrLineVoltageMetingen = 0
    avgLineVoltage = 0
    batteryVoltage = None
    batteryCharge = None
    upsLoad = None
    laagsteLineVoltage = 1000
    hoogsteLineVoltage = -1000
    
    if len(usageTable) > 0:
        for row in usageTable:
            tijdHuidig = row[0]
            if batteryVoltage == None:
                batteryVoltage = row[1]
            lineVoltage = row[2]
            if batteryCharge == None:
                batteryCharge = row[3]
            if upsLoad == None:
                upsLoad = row[4]

            if lineVoltage > hoogsteLineVoltage: 
                hoogsteLineVoltage = lineVoltage
            if lineVoltage < laagsteLineVoltage: 
                laagsteLineVoltage = lineVoltage

            sumLineVoltage += lineVoltage
            nrLineVoltageMetingen += 1
        
        datum = tijdHuidig.replace(hour=23, minute=59, second=59, microsecond=0)
        avgLineVoltage = round(sumLineVoltage / nrLineVoltageMetingen, 0)
    else:
        datum = None
    
    return (datum, batteryVoltage, avgLineVoltage, batteryCharge, upsLoad, laagsteLineVoltage, hoogsteLineVoltage)

def insertTotalsPerDay(daysBack, table):
    end = False
    dbError = False
    
    datum, batteryVoltage, avgLineVoltage, batteryCharge, upsLoad, laagsteLineVoltage, hoogsteLineVoltage  = getTotalsPerDay(daysBack, table)

    if datum != None:
        db.dbHistoryCur.execute("INSERT INTO %s (timestamp, batteryVoltage, lineVoltage, batteryCharge, upsLoad, totals, minLineVoltage, maxLineVoltage) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" % table , (datum, batteryVoltage, avgLineVoltage, batteryCharge, upsLoad, 1, laagsteLineVoltage, hoogsteLineVoltage))
    
        if db.dbHistoryCur.rowcount != 1:
            end = True
            dbError = True
            print('WARNING: Database error.')
        
        print(('Dagen terug %d: datum: %s, avgLineVoltage=%d, laagsteLineVoltage=%d, hoogsteLineVoltage=%d' % (daysBack, datum, avgLineVoltage, laagsteLineVoltage, hoogsteLineVoltage)))
    else:
        #Geen sensoren (meer) die totalen moeten krijgen
        print(("Dagen terug %d: INSERTS in tabel %s zijn klaar" % (daysBack, table)))
        db.dbHistoryCon.commit()
        end = True
        
    return (end, dbError)

def deleteAllOldMeasurements(table, nDagenNietCleanen, nDagenTerug):
                                                # "timestamp >= date('now', '-%d day') AND timestamp < date('now', '-%d day')"
    #db.dbHistoryCur.execute("DELETE FROM %s WHERE timestamp <= date('now', '-1 day') AND ((totals is null) OR (totals = '') OR (totals = '0'))" % (table))
    #
    db.dbHistoryCur.execute("DELETE FROM %s WHERE timestamp >= date('now', '-%d day') AND timestamp <= date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0'))" % (table, nDagenTerug, nDagenNietCleanen))
    rowCnt = db.dbHistoryCur.rowcount
    if rowCnt > 0:
        print(("Alle %d DELETES van  historyDB (%s) waren succesvol" % (rowCnt, table)))
        return False #Geen dbError
    else:
        print(("WARNING: %d DELETES van historyDB in tabel %s" % (rowCnt, table)))
        return True #dbError
