
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