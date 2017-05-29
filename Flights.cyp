
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

// load the flights (called legs by Max)
LOAD CSV WITH HEADERS FROM 'file:///newflights/flights.csv' AS row
WITH row, apoc.date.parse(row.EffectiveDate,"d", "M/dd/yy") AS effectiveDate,
     apoc.date.parse(row.DiscontinueDate,"d", "M/dd/yy") AS discountinueDate

WITH row,effectiveDate,discountinueDate,
     apoc.date.fields(apoc.date.format(effectiveDate, 'd', 'yyyy/MM/dd'),'yyyy/MM/dd') as mydate

WITH row.DepartureCity AS departureCity,
     row.ArrivalCity AS arrivalCity,
     row.DepartureTime AS departureTime,
     apoc.number.format(tointeger(row.ArrivalTime), '0000') AS arrivalTime,
     apoc.number.format(tointeger(row.DepartureTimezone), '0000') AS departureTimezone,
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
WITH (RANGE(0, daysActive)) AS dayrange, daysofoperation, departureCity, startDateDays,
     variationDepartureTimeCode,  variationArrivalTimeCode
UNWIND dayrange AS day
WITH day, daysofoperation, departureCity, startDateDays,variationDepartureTimeCode,  variationArrivalTimeCode
WHERE day IN daysofoperation
WITH day, daysofoperation, departureCity, startDateDays+day+variationDepartureTimeCode as departAt,
     startDateDays+day+variationArrivalTimeCode as arrivesAt
MERGE (t:Test {numb: departureCity+"-"+apoc.date.format(sday, 'd', 'yyyy-MM-dd')})
  ON CREATE set t.timestamp=timestamp(), t.cnt=0
  ON MATCH  set t.cnt = t.cnt + 1

return *   limit 10


WITH '20' AS timetest
RETURN apoc.number.format(tointeger(timetest),'0000') AS convertedtime

with apoc.date.parse("4/27/2016","s", "M/dd/yyyy") as date
return date


WITH  row.DepartureCity AS departureCity
  row.ArrivalCity as arrivalCity
  row.DepartureTime as departureTime
  apoc.number.format(tointeger(row.ArrivalTime),"0000") as arrivalTime
  apoc.number.format(tointeger(row.DepartureTimezone),"0000") as departureTimezone
  row.ArrivalTimezone as arrivalTimezone
  row.AirlineCode as airlineCode
  row.EffectiveDate as effectiveDate
  row.FlightNumber as flightNumber
  row.DayOfOperationMonday as dayMonday
  row.DayOfOperationTuesday as dayTuesday
  row.DayOfOperationWednesday as dayWednesday
  row.DayOfOperationThursday as dayThursday
  row.DayOfOperationFriday as dayFriday
  row.DayOfOperationSaturday as daySaturday
  row.DayOfOperationSunday as daySunday
  row.DiscontinueDate as discountInueDate
  row.ScheduleEffectiveDate as scheduleEffectiveDate
  row.VariationDepartureTimeCode as variationDepartureTimeCode
  row.VariationArrivalTimeCode  as variationsArrivalTimeCode
  row.FlightDistance as flightDistance




// final connect up the aiports
MATCH (a1:Airport)-[:HAS_DAY]->(ad1:AirportDay)-->
(l:Leg)-->(ad2:AirportDay)<-[:HAS_DAY]-(a2:Airport)
  WHERE a1 <> a2
WITH a1,  AVG(l.distance) AS avg_distance, a2, COUNT(*) AS flights
MERGE (a1)-[r:FLIES_TO]->(a2)
SET r.distance = avg_distance, r.flights = flights

CALL com.maxdemarzi.generateSchema();
CALL com.maxdemarzi.import.airports("/Users/mfkilgore/IdeaProjects/ic/neoflights/src/main/resources/data/airports.csv")
CALL com.maxdemarzi.import.flights("/Users/mfkilgore/IdeaProjects/ic/neoflights/src/main/resources/data/flights.csv")

with apoc.date.format(16921, 'd', 'yyyy/MM/dd') as fdate
with fdate,apoc.date.fields(fdate,'yyyy/MM/dd') as mydate
return fdate,mydate.weekdays


with apoc.date.fields("12/27/16", "M/dd/yy") as date
return date.months


//old
FOREACH (i IN RANGE(0, daysActive) |
FOREACH (activeWeekday+i IN daysofoperation |
MERGE (t:Test {numb: departureCity+"_"+efectiveDate})
  ON CREATE set t.timestamp=timestamp(), t.cnt=0
  ON MATCH  set t.cnt = t.cnt + 1