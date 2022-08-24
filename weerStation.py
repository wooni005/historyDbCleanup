import db


def getValuesPerDay(daysBack, table):
    db.dbHistoryCur.execute("SELECT timestamp, temp, hum, rain, wind FROM %s WHERE timestamp >= date('now', '-%d day') AND timestamp < date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0')) ORDER BY timestamp ASC" % (table, daysBack, (daysBack-1)))
    return db.dbHistoryCur.fetchall()


def getTotalsPerDay(daysBack, table):
    usageTable = getValuesPerDay(daysBack, table)
    lowestTemp = 1000.0
    highestTemp = -1000.0
    hardsteWind = 0
    sumRain = 0
    sumTemp = 0
    nrTempMetingen = 0
    avgTemp = 0
    nrVochtMetingen = 0
    sumVocht = 0
    vochtMeting = True
    vocht = 0
    avgVocht = 0
    tijdHuidig = 0
    if len(usageTable) > 0:
        for row in usageTable:
            tijdHuidig = row[0]
            temp = row[1]
            if row[2] is None:
                vochtMeting = False
            else:
                vocht = row[2]
            rain = row[3]
            wind = row[4]
            # strTime = tijdHuidig.strftime('%H')
            # print(tijdHuidig.strftime('%e %b %H:%M')+' mmRain:'+str(rain))
            if temp > highestTemp: 
                highestTemp = temp
            if temp < lowestTemp: 
                lowestTemp = temp
            sumTemp += temp
            nrTempMetingen += 1
            if vochtMeting:
                sumVocht += vocht
                nrVochtMetingen += 1

            sumRain += rain
                
            if wind > hardsteWind:
                hardsteWind = wind
        datum = tijdHuidig.replace(hour=23, minute=59, second=59, microsecond=0)
        
        lowestTemp = round(lowestTemp, 1)
        highestTemp = round(highestTemp, 1)
        avgTemp = round(sumTemp / nrTempMetingen, 1)
        if vochtMeting:
            avgVocht = round(sumVocht / nrVochtMetingen, 0)
        else:
            avgVocht = None
        hardsteWind = round(hardsteWind, 1)
    else:
        datum = None
    
    return datum, avgTemp, avgVocht, lowestTemp, highestTemp, sumRain, hardsteWind


def insertTotalsPerDay(daysBack, table):
    end = False
    dbError = False
    
    datum, avgTemp, avgVocht, lowestTemp, highestTemp, sumRain, hardsteWind = getTotalsPerDay(daysBack, table)

    if datum is not None:
        db.dbHistoryCur.execute("INSERT INTO %s (timestamp, temp, hum, rain, wind, totals, minTemp, maxTemp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)" % table, (datum, avgTemp, avgVocht, sumRain, hardsteWind, 1, lowestTemp, highestTemp))
    
        if db.dbHistoryCur.rowcount != 1:
            end = True
            dbError = True
            print('WARNING: Database error.')
        
        # Zet avgVocht op 0 voor debug print eronder
        if avgVocht is None:
            avgVocht = 0
        print(('Dagen terug %d: datum: %s, avgTemp=%1.1f, avgVocht=%1.1f, lowestTemp=%1.1f, highestTemp=%1.1f, sumRain=%1.2f, hardsteWind=%1.1f km/h' % (daysBack, datum, avgTemp, avgVocht, lowestTemp, highestTemp, sumRain, hardsteWind)))
                    
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
        return False  # Geen dbError
    else:
        print(("WARNING: %d DELETES van historyDB in tabel %s" % (rowCnt, table)))
        return True  # dbError
