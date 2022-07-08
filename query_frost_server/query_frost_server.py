import requests
import matplotlib.pyplot as plt

# This script queries observations for a given IOT location from the Frost server and
# outputs those observations to a CSV file. It also plots those observations with the
# given phenomenon times using the Matplotlib library. The user sets configurations
# for the Frost server URL, IOT ID, total observations to read, number of plot time ticks,
# and the observation results' units.


################################
# Configurations set by user

# Do not include forward slash at end of the URL

# Development Server
FROST_SERVER_URL = "http://34.106.60.230:8080/FROST-Server/v1.1"

# Production Server
#FROST_SERVER_URL = "https://st2.newmexicowaterdata.org/FROST-Server/v1.1"

# Same as location id
IOT_ID = "111129"

TOTAL_OBSERVATIONS_TO_READ = 1500

# Set the number of time ticks for the plot
NUMBER_OF_PLOT_TIME_TICKS = 10

RESULT_UNITS = "CMS"

################################

def build_csv_results(response_json_values, csv_row_counter):
    outfile = 'out.csv'
    with open(outfile, 'a') as wfile:
        headerkeys = ['phenomenonTime', 'result']

        if csv_row_counter == 0:
            wfile.write(f"{','.join(headerkeys)}\n")

        for ri in response_json_values[1:]:
            if csv_row_counter < TOTAL_OBSERVATIONS_TO_READ:
                row = ','.join([str(ri[k]) for k in headerkeys])
                row = f'{row}\n'
                wfile.write(row)
                csv_row_counter += 1

    return csv_row_counter


def parse_results(response_json_values, phenomenonTime_list, result_list):
    for record in response_json_values:
        if len(phenomenonTime_list) < TOTAL_OBSERVATIONS_TO_READ:
            phenomenonTime_list.append(record["phenomenonTime"])
            result_list.append(record["result"])


def plot_results(phenomenonTime_list, result_list):
    plt.plot(phenomenonTime_list, result_list)
    plt.title(f"Observed Results for {IOT_ID}")
    plt.xlabel("Phenomenon Time")
    plt.ylabel(f"Observed Result ({RESULT_UNITS})")
    plt.xticks(phenomenonTime_list, rotation=315)
    plt.locator_params(axis='x', nbins=NUMBER_OF_PLOT_TIME_TICKS)
    plt.tight_layout()
    plt.show()


def query_frost_server():
    phenomenonTime_list = []
    result_list = []
    csv_row_counter = 0

    # Use the API to limit the number of observations
    observation_url = f"{FROST_SERVER_URL}/Datastreams({IOT_ID})/Observations?$top={TOTAL_OBSERVATIONS_TO_READ}"
    r = requests.get(observation_url)
    response_json = r.json()
    response_json_values = response_json["value"]
    parse_results(response_json_values, phenomenonTime_list, result_list)

    csv_row_counter = build_csv_results(response_json_values, csv_row_counter)

    while len(phenomenonTime_list) < TOTAL_OBSERVATIONS_TO_READ:
        try:
            iot_next_link = response_json["@iot.nextLink"]

        except (IndexError, ValueError, KeyError) as e:
            print(f'exception e: {e}')
            break

        r = requests.get(iot_next_link)
        response_json = r.json()
        response_json_values = response_json["value"]

        parse_results(response_json_values, phenomenonTime_list, result_list)

        csv_row_counter = build_csv_results(response_json_values, csv_row_counter)

    plot_results(phenomenonTime_list, result_list)


if __name__ == '__main__':
    query_frost_server()

# ============= EOF =============================================
