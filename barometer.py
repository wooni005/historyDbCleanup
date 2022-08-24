import db


def getValuesPerDay(daysBack, table):
    db.dbHistoryCur.execute("SELECT timestamp, mbar FROM %s WHERE timestamp >= date('now', '-%d day') AND timestamp < date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0')) ORDER BY timestamp" % (table, daysBack, (daysBack-1)))
    return db.dbHistoryCur.fetchall()


def getTotalsPerDay(daysBack, table):
    usageTable = getValuesPerDay(daysBack, table)
    
    sumDruk = 0
    nrDrukMetingen = 0
    avgDruk = 0
    tijdHuidig = None
    
    if len(usageTable) > 0:
        for row in usageTable:
            tijdHuidig = row[0]
            druk = row[1]

            sumDruk += druk
            nrDrukMetingen += 1
        
        datum = tijdHuidig.replace(hour=23, minute=59, second=59, microsecond=0)
        avgDruk = round(sumDruk / nrDrukMetingen, 1)
    else:
        datum = None
    
    return datum, avgDruk


def insertTotalsPerDay(daysBack, table):
    end = False
    dbError = False
    
    datum, avgDruk = getTotalsPerDay(daysBack, table)

    if datum is not None:
        db.dbHistoryCur.execute("INSERT INTO %s (timestamp, mbar, totals) VALUES (?, ?, ?)" % table, (datum, avgDruk, 1))
    
        if db.dbHistoryCur.rowcount != 1:
            end = True
            dbError = True
            print('WARNING: Database error.')
        
        print(('Dagen terug %d: datum: %s, avgDruk=%d' % (daysBack, datum, avgDruk)))
    else:
        # Geen sensoren (meer) die totalen moeten krijgen
        print(("Dagen terug %d: INSERTS in tabel %s zijn klaar" % (daysBack, table)))
        db.dbHistoryCon.commit()
        end = True
        
    return end, dbError


def deleteAllOldMeasurements(table, nDagenNietCleanen, nDagenTerug):
    db.dbHistoryCur.execute("DELETE FROM %s WHERE timestamp >= date('now', '-%d day') AND timestamp <= date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0'))" % (table, nDagenTerug, nDagenNietCleanen))
    # db.dbHistoryCur.execute("DELETE FROM %s WHERE timestamp <= date('now', '-1 day') AND ((totals is null) OR (totals = '') OR (totals = '0'))" % (table))
    rowCnt = db.dbHistoryCur.rowcount
    if rowCnt > 0:
        print(("Alle %d DELETES van  historyDB (%s) waren succesvol" % (rowCnt, table)))
        return False  # dbError
    else:
        print(("WARNING: %d DELETES van historyDB in tabel %s" % (rowCnt, table)))
        return True  # dbError
