import db


def getValuesPerDay(daysBack, table, deviceName):
    #print('getValuesPerDay: '+deviceName)
    #print("SELECT timestamp, temp, hum FROM %s WHERE (deviceName == '%s') AND timestamp >= date('now', '-%d day') AND timestamp < date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0')) ORDER BY timestamp" % (table, deviceName, daysBack, (daysBack - 1)))
    db.dbHistoryCur.execute("SELECT timestamp, temp, hum FROM %s WHERE (deviceName == '%s') AND timestamp >= date('now', '-%d day') AND timestamp < date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0')) ORDER BY timestamp" % (table, deviceName, daysBack, (daysBack - 1)))
    return db.dbHistoryCur.fetchall()


def getTotalsPerDay(daysBack, table, deviceName):
    usageTable = getValuesPerDay(daysBack, table, deviceName)
    laagsteTemp = 1000.0
    hoogsteTemp = -1000.0
    sumTemp = 0
    nrTempMetingen = 0
    nrVochtMetingen = 0
    sumVocht = 0
    vochtMeting = True
    vocht = 0
    if len(usageTable) > 0:
        for row in usageTable:
            try:
                tijdHuidig = row[0]
                temp = float(row[1])
                if row[2] is None:
                    vochtMeting = False
                else:
                    vocht = row[2]
                #strTime = tijdHuidig.strftime('%H')
                #print('sensor:'+deviceName+': '+tijdHuidig.strftime('%e %b %H:%M')+' '+str(temp)+' C '+'vocht:'+str(vocht))
                if temp > hoogsteTemp:
                    hoogsteTemp = temp
                if temp < laagsteTemp:
                    laagsteTemp = temp
                sumTemp += temp
                nrTempMetingen += 1
                if vochtMeting:
                    sumVocht += vocht
                    nrVochtMetingen += 1
            except Exception as e:
                strTime = row[0].strftime('%e %b %H:%M')
                print("WARNING: Skipping row: metingTemp: %s: time: %s value: %s Error: %s" % (deviceName, strTime, str(row[1]), str(e)))

        datum = tijdHuidig.replace(hour=23, minute=59, second=59, microsecond=0)

        laagsteTemp = round(laagsteTemp, 1)
        hoogsteTemp = round(hoogsteTemp, 1)
        avgTemp = round(sumTemp / nrTempMetingen, 1)
        if vochtMeting:
            avgVocht = round(sumVocht / nrVochtMetingen, 0)
        else:
            avgVocht = None
    else:
        datum = None

    return (datum, avgTemp, avgVocht, laagsteTemp, hoogsteTemp)


def insertTotalsPerDay(daysBack, table):
    end = False
    dbError = False

    #Haal alle deviceNamen op die totalen moeten krijgen
    db.dbHistoryCur.execute("SELECT DISTINCT deviceName FROM %s WHERE timestamp >= date('now', '-%d day') AND timestamp < date('now', '-%d day') AND ((totals is null) OR (totals = '') OR (totals = '0')) ORDER BY timestamp" % (table, daysBack, (daysBack - 1)))
    sensorTable = db.dbHistoryCur.fetchall()

    #print(sensorTable)

    if len(sensorTable) > 0:
        for row in sensorTable:
            deviceName = row[0]

            datum, avgTemp, avgVocht, laagsteTemp, hoogsteTemp  = getTotalsPerDay(daysBack, table, deviceName)

            if datum is not None:
                db.dbHistoryCur.execute("INSERT INTO %s (timestamp, deviceName, temp, hum, totals, minTemp, maxTemp) VALUES (?, ?, ?, ?, ?, ?, ?)" % table, (datum, deviceName, avgTemp, avgVocht, 1, laagsteTemp, hoogsteTemp))

                if db.dbHistoryCur.rowcount != 1:
                    end = True
                    dbError = True
                    print(('WARNING: Database error. deviceName: %s', deviceName))

                #Zet avgVocht op 0 voor de debug print eronder
                if avgVocht is None:
                    avgVocht = 0
                print(('Dagen terug %d: datum: %s, deviceName: %s: avgTemp=%1.1f, avgVocht=%1.1f, laagsteTemp=%1.1f, hoogsteTemp=%1.1f' % (daysBack, datum, deviceName, avgTemp, avgVocht, laagsteTemp, hoogsteTemp)))

    else:
        #Geen sensoren (meer) die totalen moeten krijgen
        print(("Dagen terug %d: INSERTS in tabel %s zijn klaar" % (daysBack, table)))
        db.dbHistoryCon.commit()
        end = True

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
