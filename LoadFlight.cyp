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



return apoc.date.format(1462076400,"s","yyyy/MM/dd HHmm")