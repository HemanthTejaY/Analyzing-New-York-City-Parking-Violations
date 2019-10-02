# Analyzing New York City Parking Violations

##### DATA
The data we will use is open and available from NYC Open Data:

### Parking Violations Issued (Fiscal Year 2016)
https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2016/kiv2-tbus/data
### Open Parking and Camera Violations
https://data.cityofnewyork.us/City-Government/Open-Parking-and-CameraViolations/nc67-uf89

We use SQL, Spark and Hadoop technologies to analyze the NYC Open data. These technologies help us attain some useful insights about the parking and open violations in New York City. 

#### The tasks we compute are as follows:-

##### Problem 1
Find all parking violations that have been paid, i.e., that do not occur in openviolations.csv

##### Problem 2
Find the frequencies of the violation types in parking_violations.csv, i.e., for each
violation code, the number of violations that this code has

##### Problem 3
Find the total and average amounts due in open violations for each license type

##### Problem 4
Compute the total number of violations for vehicles registered in the state of NY and
all other vehicles.

##### Problem 5
 Find the vehicle that has had the greatest number of violations

##### Problem 6
Find the top-20 vehicles in terms of total violations

##### Problem 7
For each violation code, list the average number of violations with that code issued per day on weekdays and weekend days. You may hardcode “8” as the number of weekend days and “23” as the number of weekdays in March 2016. In March 2016, the 5th, 6th, 12th, 13th, 19th, 20th, 26th, and 27th were weekend days (i.e., Sat. and Sun.).
