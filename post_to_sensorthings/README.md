The post_to_sensorthings.py script queries location and associated observation 
data from a table in the Google Cloud Platform (GCP) BigQuery data warehouse and 
posts that location and observation data to a FROST-Server implementing the Open 
Geosptatial Consortium (OGC) SensorThings (ST) API.  Following the ST standard, 
this script creates a single Location entity and Thing entity for each location 
read from BiqQuery. Each Thing has four assiciated Datastream entities for the 
different types of observations, and each Datastream holds the full time series 
of Observations for that Datastream, Thing, and Location. Therefore, when each 
observation is read from BigQuery, an Observation entity is created and posted 
to its associated Datastream with given times for phenomenonTime and resultTime.

This example uses water totalizer data from the New Mexico Interstate Stream Commission (ISC)
API that has already been loaded into a BigQuery table.


[OGC SensorThings API Standards](https://www.ogc.org/standards/sensorthings)

[OGC SensorThings API Developer Documentation](https://developers.sensorup.com/docs/#introduction)

[FROST-Server Repository and Documentation](https://github.com/FraunhoferIOSB/FROST-Server)
