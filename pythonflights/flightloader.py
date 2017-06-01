from datetime import datetime, timedelta

import pandas as pd
from neo4j.v1 import GraphDatabase

airportconstraint = '''
CREATE  CONSTRAINT ON (ap:Airport) ASSERT ap.code IS UNIQUE
'''

airportdaycontrant = '''
CREATE  CONSTRAINT ON (aday:AirportDay) ASSERT aday.key IS UNIQUE
'''

airportdata = '''
MERGE (ap:Airport {code: {id}}) 
   ON CREATE SET ap.country = {Country},
      ap.latitude = {Latitude},
      ap.longitude = {Longitude},
      ap.lat = {Lat},
      ap.lon = {Lon},
      ap.name = {Name}
'''

airportday = '''
MERGE (a:AirportDay {key:{key}})
'''

airport_airportday = '''
MATCH (a:Airport {code:{code}}), (ad:AirportDay {key:{key}}) MERGE (a)-[:HAS_DAY]->(ad)
'''

leg = '''
CREATE (l:Leg { code:{code}, departs:{departs}, arrives:{arrives}, distance:{distance} })
with l
MATCH (ad:AirportDay {key:{keyDeparts}})
MATCH (aa:AirportDay {key:{keyArrives}})
CALL apoc.create.relationship(l,{reltype}, {}, aa) yield rel AS REL1
CALL apoc.create.relationship(ad,{reltype}, {}, l) yield rel AS REL2
RETURN COUNT(l)
'''

# final step
connectairports = '''
MATCH (a1:Airport)-[:HAS_DAY]->(ad1:AirportDay)-->
(l:Leg)-->(ad2:AirportDay)<-[:HAS_DAY]-(a2:Airport)
  WHERE a1 <> a2
WITH a1,  AVG(l.distance) AS avg_distance, a2, COUNT(*) AS flights
MERGE (a1)-[r:FLIES_TO]->(a2)
SET r.distance = avg_distance, r.flights = flights
'''

driver = GraphDatabase.driver("bolt://localhost", encrypted=False)
session = driver.session()


def main():
    # constraints
    result = session.run(airportconstraint)
    summary = result.summary()
    print('Counters: ', summary.counters)

    result = session.run(airportdaycontrant)
    summary = result.summary()
    print('Counters: ', summary.counters)

    airports()
    flights()


def airports():
    df = pd.read_csv('/Users/mfkilgore/IdeaProjects/ic/neoflights/src/main/resources/data/airports.csv')
    for index, row in df.iterrows():
        print(row.Code)
        try:
            result = session.run(airportdata, {'id': row.Code, 'Country': row.Country, 'Latitude': row.Latitude,
                                               'Longitude': row.Longitude, 'Lat': row.Lat, 'Lon': row.Lon,
                                               'Name': row.Name})
            summary = result.summary()
            print('Counters: ', summary.counters)
        except Exception as e:
            print('general error ', e)


def flights():
    df = pd.read_csv('/Users/mfkilgore/IdeaProjects/ic/neoflights/src/main/resources/data/flights.csv')
    for index, row in df.iterrows():
        #  calculate fields
        departuretime = '{:0>4}'.format(row.DepartureTime)
        arrivaltime = '{:0>4}'.format(row.ArrivalTime)
        print(row.DepartureCity)
        print(departuretime)

        arrivallocaltime = datetime.strptime(arrivaltime, "%H%M")
        effectivedate = datetime.strptime(row.EffectiveDate, "%m/%d/%y")
        fulleffectivedate = effectivedate + timedelta(hours=arrivallocaltime.hour, minutes=arrivallocaltime.minute)

        departurelocaltime = datetime.strptime(departuretime, "%H%M")
        discontinueDate = datetime.strptime(row.DiscontinueDate, "%m/%d/%y")
        fulldiscontinueDate = discontinueDate + timedelta(hours=departurelocaltime.hour,
                                                          minutes=departurelocaltime.minute)
        print(effectivedate)

        daysOfOperation = []
        if not pd.isnull(row.DayOfOperationMonday):
            daysOfOperation.append(1)
        if not pd.isnull(row.DayOfOperationTuesday):
            daysOfOperation.append(2)
        if not pd.isnull(row.DayOfOperationWednesday):
            daysOfOperation.append(3)
        if not pd.isnull(row.DayOfOperationThursday):
            daysOfOperation.append(4)
        if not pd.isnull(row.DayOfOperationFriday):
            daysOfOperation.append(5)
        if not pd.isnull(row.DayOfOperationSaturday):
            daysOfOperation.append(6)
        if not pd.isnull(row.DayOfOperationSunday):
            daysOfOperation.append(7)

        print(daysOfOperation)

        daysBetween = discontinueDate - effectivedate

        print(daysBetween)
        nextdate = effectivedate
        for i in range(daysBetween.days):
            # add if days of service
            print(daysBetween.days)
            nextdate = effectivedate + timedelta(days=i)
            weekday = nextdate.weekday() + 1
            if not weekday in daysOfOperation:
                continue
            # add the time
            departure = nextdate + timedelta(hours=departurelocaltime.hour, minutes=departurelocaltime.minute)
            arrival = nextdate + + timedelta(hours=arrivallocaltime.hour, minutes=arrivallocaltime.minute)
            departureKey = row.DepartureCity + "-" + '{0:%Y-%m-%d}'.format(departure)
            print(departureKey)
            arrivalKey = row.ArrivalCity + "-" + '{0:%Y-%m-%d}'.format(arrival)
            print(arrivalKey)
            with session.begin_transaction() as tx:
                try:
                    result = tx.run(airportday, {'key': departureKey})
                    summary = result.summary()
                    print('Counters: ', summary.counters)
                except Exception as e:
                    print('general error ', e)
                try:
                    result = tx.run(airportday, {'key': arrivalKey})
                    summary = result.summary()
                    print('Counters: ', summary.counters)
                except Exception as e:
                    print('general error ', e)
                try:
                    result = tx.run(airport_airportday, {'code': row.DepartureCity, 'key': departureKey})
                    summary = result.summary()
                    print('Counters: ', summary.counters)
                except Exception as e:
                    print('general error ', e)
                try:
                    result = tx.run(airport_airportday, {'code': row.ArrivalCity, 'key': arrivalKey})
                    summary = result.summary()
                    print('Counters: ', summary.counters)
                except Exception as e:
                    print('general error ', e)
                try:
                    result = tx.run(leg, {'code': row.AirlineCode + "-" + str(row.FlightNumber),
                                               'departs': '{0:%Y-%m-%d %H:%M}'.format(departure),
                                               'arrives': '{0:%Y-%m-%d %H:%M}'.format(arrival),
                                               'keyArrives': arrivalKey,
                                               'keyDeparts': departureKey,
                                               'reltype': row.ArrivalCity + '_FLIGHT',
                                               'distance': row.FlightDistance})
                    summary = result.summary()
                    print('Counters: ', summary.counters)
                except Exception as e:
                    print('general error ', e)


main()
session.close()
