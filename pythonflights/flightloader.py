
import csv
from neo4j.v1 import GraphDatabase

airportconstraint = '''
CREATE  CONSTRAINT ON (ap:Airport) ASSERT ap.code IS UNIQUE
'''

airportdaycontrant = '''
CREATE  CONSTRAINT ON (aday:AirportDay) ASSERT aday.key IS UNIQUE
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
    session.run(airportconstraint)
    session.run(airportdaycontrant)
    # aiport csv file
    with open('/Users/mfkilgore/IdeaProjects/ic/neoflights/src/main/resources/data/airports.csv') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',')
        next(csvreader)
        for row in csvreader:
            print(row[0],row[1])
            #for col in row:
            #    print()

main()
session.close()