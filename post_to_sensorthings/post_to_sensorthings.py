from google.cloud import bigquery
from google.oauth2 import service_account
import requests
from datetime import datetime

# This script queries location and associated observation data from a table in the
# Google Cloud Platform (GCP) BigQuery data warehouse and posts that location and
# observation data to a FROST-Server implementing the Open Geosptatial Consortium (OGC)
# SensorThings (ST) API.  Following the ST standard, this script creates a single
# Location entity and Thing entity for each location read from BiqQuery. Each Thing
# has four assiciated Datastream entities for the different types of observations,
# and each Datastream holds the full time series of Observations for that Datastream,
# Thing, and Location. Therefore, when each observation is read from BigQuery, an
# Observation entity is created and posted to its associated Datastream with given
# times for phenomenonTime and resultTime.

# This example uses water totalizer data from the New Mexico Interstate Stream Commission (ISC)
# API that has already been loaded into a BigQuery table.

# References:
# https://www.ogc.org/standards/sensorthings
# https://developers.sensorup.com/docs/#introduction
# https://github.com/FraunhoferIOSB/FROST-Server


################################
# Configurations set by user
AGENCY = 'ISC'

THINGS_URL = 'http://.../FROST-Server/v1.1/Things'
OBSERVATIONS_URL = 'http://.../FROST-Server/v1.1/Observations'

# Credentials to a GCP BigQuery table
CREDENTIALS = service_account.Credentials.from_service_account_file("")

################################

def make_geometry_point_from_latlon(lat, lon):
    return {"type": "Point", "coordinates": [float(lon), float(lat)]}


# Get location data from BigQuery and post to SensorThings API
def get_location_data():

    client = bigquery.Client(credentials=CREDENTIALS)

    sql = 'select * from locations.isc_seven_rivers_monitoring_points'

    job = client.query(sql)

    result = job.result()

    for record in result:
        loc = make_geometry_point_from_latlon(record['latitude'], record['longitude'])

        # Properties
        props = {'source_id': record['id'],
                 'agency': AGENCY,
                 'source_api': 'https://nmisc-wf.gladata.com/api/',
                 'observation_category': 'totalizer'}

        # Location entity
        location_obj = {'name': record['name'],
                'description': 'No Description',
                'location': loc,
                'properties': props,
                "encodingType": "application/vnd.geo+json", }

        # Datastream entity
        datastream_obj_rateKWH = {'name': 'Totalizer DS rateKWH',
                'description': 'Datastream for Totalizer rateKWH',
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "unitOfMeasurement": {
                    "name": "kilowatt hour",
                    "symbol": "KWH",
                    "definition": "https://qudt.org/vocab/unit/KiloW-HR"
                },
                "ObservedProperty": {
                    "name": "rateKWH",
                    "description": "rate in KWH of totalizer",
                    "definition": ""
                },
                "Sensor": {
                    "name": "NoSensor",
                    "description": "No description",
                    "encodingType": "application/pdf",
                    "metadata": "No Metadata"
                }
        }

        # Datastream entity
        datastream_obj_srfVolumeKWH = {'name': 'Totalizer DS srfVolumeKWH',
                'description': 'Datastream for Totalizer srfVolumeKWH',
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "unitOfMeasurement": {
                    "name": "kilowatt hour",
                    "symbol": "KWH",
                    "definition": "https://qudt.org/vocab/unit/KiloW-HR"
                },
                "ObservedProperty": {
                    "name": "srfVolumeKWH",
                    "description": "surface volume in KWH of totalizer",
                    "definition": ""
                },
                "Sensor": {
                    "name": "NoSensor",
                    "description": "No description",
                    "encodingType": "application/pdf",
                    "metadata": "No Metadata"
                }
        }

        # Datastream entity
        datastream_obj_nmiscVolumeKWH = {'name': 'Totalizer DS nmiscVolumeKWH',
                'description': 'Datastream for Totalizer nmiscVolumeKWH',
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "unitOfMeasurement": {
                    "name": "kilowatt hour",
                    "symbol": "KWH",
                    "definition": "https://qudt.org/vocab/unit/KiloW-HR"
                },
                "ObservedProperty": {
                    "name": "nmiscVolumeKWH",
                    "description": "rate in KWH of NM ISC",
                    "definition": ""
                },
                "Sensor": {
                    "name": "NoSensor",
                    "description": "No description",
                    "encodingType": "application/pdf",
                    "metadata": "No Metadata"
                }
        }

        # Datastream entity
        datastream_obj_otherVolumeKWH = {'name': 'Totalizer DS otherVolumeKWH',
                'description': 'Datastream for Totalizer otherVolumeKWH',
                "observationType": "http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement",
                "unitOfMeasurement": {
                    "name": "kilowatt hour",
                    "symbol": "KWH",
                    "definition": "https://qudt.org/vocab/unit/KiloW-HR"
                },
                "ObservedProperty": {
                    "name": "otherVolumeKWH",
                    "description": "rate in KWH of other Volume",
                    "definition": ""
                },
                "Sensor": {
                    "name": "NoSensor",
                    "description": "No description",
                    "encodingType": "application/pdf",
                    "metadata": "No Metadata"
                }
        }

        # Full object to be posted; creating a Thing, Location, and
        # four associated Datastreams
        obj = {'name': 'Totalizer Monitoring System',
                'description': 'ISC Totalizer Observations',
                'properties': props,
                'Locations': [location_obj],
                'Datastreams': [datastream_obj_rateKWH,
                                datastream_obj_srfVolumeKWH,
                                datastream_obj_nmiscVolumeKWH,
                                datastream_obj_otherVolumeKWH
                               ]
              }

        x = requests.post(URL, json=obj)
    return


# Post observation data to Sensor Things API
def post_observation(source_id_to_datastream_id_dict, record, time_utc_iso, observation_name):

        datastream_id = source_id_to_datastream_id_dict[int(record['monitoring_point_id'])][observation_name]

        observation_obj = {'phenomenonTime': time_utc_iso,
                           'resultTime': time_utc_iso,
                           'result': record[observation_name],
                           'Datastream':{"@iot.id":datastream_id}
                          }

        x = requests.post(OBSERVATIONS_URL, json=observation_obj)
        return


# Get observation data from BiqQuery
def get_observation_data():

    source_id_to_iot_id_dict = {}

    orderby = "?$orderby=id%20desc"
 
    things_url_orderby = f"{THINGS_URL}{orderby}"

    r = requests.get(things_url_orderby)

    response_json = r.json()

    response_json_values = response_json["value"]

    for thing in response_json_values:
        try:
            if thing['properties']['observation_category'] == 'totalizer':
                source_id_to_iot_id_dict[int(thing['properties']['source_id'])] = int(thing['@iot.id'])
        except:
            pass

    source_id_to_datastream_id_dict = {}

    for key_source_id, value_thing_iot_id in source_id_to_iot_id_dict.items():

        datastreams_url = f"{URL}({value_thing_iot_id})/Datastreams"

        r = requests.get(datastreams_url)

        response_json = r.json()

        response_json_values = response_json["value"]

        source_id_to_datastream_id_dict[key_source_id] = {}

        for datastream in response_json_values:

            if "nmiscVolumeKWH" in datastream["name"]:
                source_id_to_datastream_id_dict[key_source_id]["nmiscVolumeKWH"] = datastream["@iot.id"]

            elif "srfVolumeKWH" in datastream["name"]:
                source_id_to_datastream_id_dict[key_source_id]["srfVolumeKWH"] = datastream["@iot.id"]

            elif "otherVolumeKWH" in datastream["name"]:
                source_id_to_datastream_id_dict[key_source_id]["otherVolumeKWH"] = datastream["@iot.id"]

            elif "rateKWH" in datastream["name"]:
                source_id_to_datastream_id_dict[key_source_id]["rateKWH"] = datastream["@iot.id"]

    client = bigquery.Client(credentials=CREDENTIALS)

    sql = 'select * from levels.isc_totalizer_readings'

    job = client.query(sql)

    result = job.result()

    for record in result:

        #Convert UTC timestamp to ISO 8601
        time_utc_iso = datetime.utcfromtimestamp(int(record['dateTime']/1000)).isoformat(timespec='milliseconds') + 'Z'
       
        post_observation(source_id_to_datastream_id_dict, record, time_utc_iso, 'nmiscVolumeKWH')
        post_observation(source_id_to_datastream_id_dict, record, time_utc_iso, 'srfVolumeKWH')
        post_observation(source_id_to_datastream_id_dict, record, time_utc_iso, 'otherVolumeKWH')
        post_observation(source_id_to_datastream_id_dict, record, time_utc_iso, 'rateKWH')

        return


if __name__ == '__main__':
    get_location_data()
    get_observation_data()

# ============= EOF =============================================
