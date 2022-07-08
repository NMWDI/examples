The query_frost_server.py script queries observations for a given IOT location from the Frost server and
outputs those observations to a CSV file. It also plots those observations with the
given phenomenon times using the Matplotlib library. The user sets configurations
for the Frost server URL, IOT ID, total observations to read, number of plot time ticks,
and the observation results' units.

[An example CSV of the phenomenon times and observations.](query_frost_server/example_results/out.csv)

Below is an example plot of 1,500 observations for a given IOT location: 

![Plot](example_results/Observed_Resuts_for_111129.png?raw=true "Title")
