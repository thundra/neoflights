
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
WITH row.DepartureCity AS departureCity,
     row.ArrivalCity AS arrivalCity,
     row.DepartureTime AS departureTime,
     apoc.number.format(tointeger(row.ArrivalTime), '0000') AS arrivalTime,
     apoc.number.format(tointeger(row.DepartureTimezone), '0000') AS departureTimezone,
     row.ArrivalTimezone AS arrivalTimezone,
     row.AirlineCode AS airlineCode,
     row.EffectiveDate AS effectiveDate,
     row.FlightNumber AS flightNumber,
     coalesce(row.DayOfOperationMonday, 1) AS dayMonday,
     coalesce(row.DayOfOperationTuesday, 2) AS dayTuesday,
     coalesce(row.DayOfOperationWednesday, 3) AS dayWednesday,
     coalesce(row.DayOfOperationThursday, 4) AS dayThursday,
     coalesce(row.DayOfOperationFriday, 5) AS dayFriday,
     coalesce(row.DayOfOperationSaturday, 6) AS daySaturday,
     coalesce(row.DayOfOperationSunday, 7) AS daySunday,
     row.DiscontinueDate AS discountInueDate,
     row.ScheduleEffectiveDate AS scheduleEffectiveDate,
     row.VariationDepartureTimeCode AS variationDepartureTimeCode,
     row.VariationArrivalTimeCode  AS variationsArrivalTimeCode,
     row.FlightDistance AS flightDistance

  LIMIT 10

RETURN *


WITH '20' AS timetest
RETURN apoc.number.format(tointeger(timetest),'0000') AS convertedtime


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
