import db


def getValuesPerDay(daysBack, table):
    db.dbHistoryCur.execute("SELECT timestamp, totalKWHpulsesI, totalKWHpulsesII, addedKWHpulsesI, addedKWHpulsesII FROM %s WHERE timestamp >= date('now', '-%d day') AND timestamp < date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0')) ORDER BY timestamp" % (table, daysBack, (daysBack-1)))
    return db.dbHistoryCur.fetchall()


def getTotalsPerDay(daysBack, table):
    usageTable = getValuesPerDay(daysBack, table)
    addedKWHpulsesI = 0  # high tarif
    addedKWHpulsesII = 0  # low tarif
    totalKWHpulsesI = 0
    totalKWHpulsesII = 0
    datum = None
    firstRow = True
    row = None

    if len(usageTable) > 0:
        for row in usageTable:
            if firstRow:
                firstRow = False
                # For the totoal counters we need only the first row
                totalKWHpulsesI = row[1]
                totalKWHpulsesII = row[2]
            
            # sum the pulses for the whole day
            addedKWHpulsesI += row[3]
            addedKWHpulsesII += row[4]
            
        datum = row[0].replace(hour=23, minute=59, second=59, microsecond=0)

    return datum, totalKWHpulsesI, totalKWHpulsesII, addedKWHpulsesI, addedKWHpulsesII


def insertTotalsPerDay(daysBack, table):
    end = False
    dbError = False
    
    datum, totalKWHpulsesI, totalKWHpulsesII, addedKWHpulsesI, addedKWHpulsesII = getTotalsPerDay(daysBack, table)
    
    # Check of dit de laatste/oudste dag was
    if datum is None:
        print(("Dagen terug %d: INSERTS in tabel %s zijn klaar" % (daysBack, table)))
        db.dbHistoryCon.commit()
        end = True
    else:
        db.dbHistoryCur.execute("INSERT INTO %s (timestamp, watt, totalKWHpulsesI, totalKWHpulsesII, addedKWHpulsesI, addedKWHpulsesII, totals) VALUES (?, ?, ?, ?, ?, ?, ?)" % table, (datum, 0, totalKWHpulsesI, totalKWHpulsesII, addedKWHpulsesI, addedKWHpulsesII, 1))
    
        if db.dbHistoryCur.rowcount != 1:
            end = True
            dbError = True
        print(('Dagen terug %d: datum: %s, totalKWHpulsesI=%d, totalKWHpulsesII=%d, addedKWHpulsesI=%d, addedKWHpulsesII=%d' % (daysBack, datum, totalKWHpulsesI, totalKWHpulsesII, addedKWHpulsesI, addedKWHpulsesII)))
    
    return end, dbError


def deleteAllOldMeasurements(table, nDagenNietCleanen, nDagenTerug):
    db.dbHistoryCur.execute("DELETE FROM %s WHERE timestamp >= date('now', '-%d day') AND timestamp <= date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0'))" % (table, nDagenTerug, nDagenNietCleanen))
    # db.dbHistoryCur.execute("DELETE FROM %s WHERE timestamp <= date('now', '-1 day') AND ((totals is null) OR (totals = '') OR (totals = '0'))" % (table))
    rowCnt = db.dbHistoryCur.rowcount
    if rowCnt > 0:
        print(("Alle %d DELETES van  historyDB (%s) waren succesvol" % (rowCnt, table)))
        return False  # Geen dbError
    else:
        print(("WARNING: %d DELETES van historyDB in tabel %s" % (rowCnt, table)))
        return True  # dbError
