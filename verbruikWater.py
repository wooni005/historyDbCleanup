import db

def getValuesPerDay(daysBack, table):
    db.dbHistoryCur.execute("SELECT timestamp, liter FROM %s WHERE timestamp >= date('now', '-%d day') AND timestamp < date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0')) ORDER BY timestamp ASC" % (table, daysBack , (daysBack-1)))
    return db.dbHistoryCur.fetchall()

def getTotalsPerDay(daysBack, table):
    usageTable = getValuesPerDay(daysBack, table)
    totaalLiters = 0
    totaalAan = 0
    if len(usageTable) > 0:
        for row in usageTable:
            #strTime = row[1].strftime('%H')
            #print row[0].strftime('%e %b %H:%M')+' '+str(row[1])+' liter'
            totaalAan += 1
            totaalLiters += row[1]
            
        datum = row[0].replace(hour=23, minute=59, second=59, microsecond=0)
    else:
        datum = None
    
    return (datum, totaalLiters, totaalAan)

def insertTotalsPerDay(daysBack, table):
    end = False
    dbError = False
    
    datum, totaalLiters, totaalAan = getTotalsPerDay(daysBack, table)
    #Check of dit de laatste/oudste dag was
    if datum == None:
        print(("Dagen terug %d: INSERTS in tabel %s zijn klaar" % (daysBack, table)))
        db.dbHistoryCon.commit()
        end = True
    else:
        db.dbHistoryCur.execute("INSERT INTO %s (timestamp, liter, nrPompAan, totals) VALUES (?, ?, ?, ?)" % table , (datum, totaalLiters, totaalAan,  1))
    
        if db.dbHistoryCur.rowcount != 1:
            end = True
            dbError = True
        print(('Dagen terug %d: datum: %s, totaalLiters=%d liter, totaalPompAan=%d' % (daysBack, datum, totaalLiters, totaalAan)))
    
    return (end, dbError)

def deleteAllOldMeasurements(table, nDagenNietCleanen, nDagenTerug):
    db.dbHistoryCur.execute("DELETE FROM %s WHERE timestamp >= date('now', '-%d day') AND timestamp <= date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0'))" % (table, nDagenTerug, nDagenNietCleanen))
    #db.dbHistoryCur.execute("DELETE FROM %s WHERE timestamp <= date('now', '-1 day') AND ((totals is null) OR (totals = '') OR (totals = '0'))" % (table))
    rowCnt = db.dbHistoryCur.rowcount
    if rowCnt > 0:
        print(("Alle %d DELETES van  historyDB (%s) waren succesvol" % (rowCnt, table)))
        return False #Geen dbError
    else:
        print(("WARNING: %d DELETES van historyDB in tabel %s" % (rowCnt, table)))
        return True #dbError
