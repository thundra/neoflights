//create the schema
CREATE  CONSTRAINT ON (ap:Airport) ASSERT ap.code IS UNIQUE
CREATE  CONSTRAINT ON (aday:AirportDay) ASSERT aday.key IS UNIQUE

// load the airports
LOAD CSV WITH HEADERS FROM 'file:///newflights/airports.csv' AS row
WITH row, tofloat(row.Lat) AS lat, tofloat(row.Lon) AS lon

MERGE (ap:Airport {code: row.Code})
  ON CREATE
  SET ap.country = row.Country,
  ap.latitude = row.Latitude,
  ap.longitude = row.Longitude,
  ap.lat = lat,
  ap.lon = lon,
  ap.name = row.name


LOAD CSV WITH HEADERS FROM 'file:///newflights/flights.csv' AS row
WITH row, apoc.date.parse(row.EffectiveDate,"d", "M/dd/yy") AS effectiveDate,
     apoc.date.parse(row.DiscontinueDate,"d", "M/dd/yy") AS discountinueDate

WITH row,effectiveDate,discountinueDate,
     apoc.date.fields(apoc.date.format(effectiveDate, 'd', 'yyyy/MM/dd'),'yyyy/MM/dd') as mydate

WITH row.DepartureCity AS departureCity,
     row.ArrivalCity AS arrivalCity,
     row.DepartureTimezone AS departureTimezone,
     apoc.number.format(tointeger(row.ArrivalTime), '0000') AS arrivalTime,
     apoc.number.format(tointeger(row.DepartureTime), '0000') AS departureTime,
     row.ArrivalTimezone AS arrivalTimezone,
     row.AirlineCode AS airlineCode,
     row.FlightNumber AS flightNumber,
     [tointeger(row.DayOfOperationMonday),
       tointeger(row.DayOfOperationTuesday),
       tointeger(row.DayOfOperationWednesday),
       tointeger(row.DayOfOperationThursday),
       tointeger(row.DayOfOperationFriday),
       tointeger(row.DayOfOperationSaturday),
       tointeger(row.DayOfOperationSunday)] as daysofoperation,
     row.DiscontinueDate as endDate,
     row.EffectiveDate as startDate,
     discountinueDate as endDateDays,
     effectiveDate as startDateDays,
     discountinueDate - effectiveDate as daysActive,
     mydate.weekdays as activeWeekDay,
     row.ScheduleEffectiveDate AS scheduleEffectiveDate,
     toInteger(row.VariationDepartureTimeCode) AS variationDepartureTimeCode,
     toInteger(row.VariationArrivalTimeCode)  AS variationArrivalTimeCode,
     row.FlightDistance AS flightDistance

/// next step - foreach like max uses looping through variables above
WITH (RANGE(0, daysActive)) AS dayrange, daysofoperation, departureCity, startDateDays, arrivalCity,
     variationDepartureTimeCode,  variationArrivalTimeCode, arrivalTime, departureTime
UNWIND dayrange AS day
WITH day, daysofoperation, departureCity, startDateDays,variationDepartureTimeCode, variationArrivalTimeCode,
     arrivalCity, arrivalTime, departureTime
  WHERE day IN daysofoperation
WITH day, daysofoperation, departureCity, startDateDays+day+variationDepartureTimeCode as departAt,
     startDateDays+day+variationArrivalTimeCode as arrivesAt, arrivalCity, arrivalTime, departureTime
MERGE (t:Test {numb: departureCity+"-"+apoc.date.format(departAt, 'd', 'yyyy-MM-dd')})
  ON CREATE set t.timestamp=timestamp(), t.cnt=0
  ON MATCH  set t.cnt = t.cnt + 1
MERGE (a:Test {numb: arrivalCity+"-"+apoc.date.format(arrivesAt, 'd', 'yyyy-MM-dd')})
  ON CREATE set a.timestamp=timestamp(), t.cnt=0
  ON MATCH  set a.cnt = t.cnt + 1
with arrivalTime, t, a, apoc.date.format(departAt,"d","yyyy/MM/dd")+" "+departureTime as depart,
     apoc.date.format(arrivesAt,"d","yyyy/MM/dd")+" "+arrivalTime as arrives
return *   limit 10



// final connect up the aiports
MATCH (a1:Airport)-[:HAS_DAY]->(ad1:AirportDay)-->
(l:Leg)-->(ad2:AirportDay)<-[:HAS_DAY]-(a2:Airport)
  WHERE a1 <> a2
WITH a1,  AVG(l.distance) AS avg_distance, a2, COUNT(*) AS flights
MERGE (a1)-[r:FLIES_TO]->(a2)
SET r.distance = avg_distance, r.flights = flights


// calling max code
CALL com.maxdemarzi.generateSchema();
CALL com.maxdemarzi.import.airports("/Users/mfkilgore/IdeaProjects/ic/neoflights/src/main/resources/data/airports.csv")
CALL com.maxdemarzi.import.flights("/Users/mfkilgore/IdeaProjects/ic/neoflights/src/main/resources/data/flights.csv")
