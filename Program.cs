using System.Text.Json;
using WeatherAPI;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

var server = Environment.GetEnvironmentVariable("DB_ENDPOINT");
var userID = Environment.GetEnvironmentVariable("DB_USER");

var portStr = Environment.GetEnvironmentVariable("DB_PORT");

uint port = 3306; // Default mariaDB port

if (portStr != null)
{
	port = uint.Parse(portStr);
}

var password = Environment.GetEnvironmentVariable("DB_PASSWORD");
var database = Environment.GetEnvironmentVariable("DB_DB");

var geocodeAPIKey = Environment.GetEnvironmentVariable("GCAPIKEY");

if (geocodeAPIKey == null)
{
	Console.WriteLine("Geocode API key not provided, location endpoints will not work.");
}

var dbBuilder = new MySqlConnector.MySqlConnectionStringBuilder
{
    Server = server,
    UserID = userID,
    Port = port,
    Password = password,
    Database = database,
};

app.MapGet("/", async () =>
{
	/// <summary>
	/// This is our main endpoint at the root of our API
	/// This endpoint retrieves weather info from all devices from the last 2 hours
	/// </summary>
	return await QueryParser.Parse(dbBuilder, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/hour", async () =>
{
	/// <summary>
	/// This endpoint returns all weather data from the last hour
	/// </summary>
	return await QueryParser.Parse(dbBuilder, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"	
	);
});

app.MapGet("/today", async () =>
{
	/// <summary>
	/// This endpoint returns all weather data from today (the current date)
	/// </summary>
	return await QueryParser.Parse(dbBuilder, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE cast(timestamp as date) = CURDATE()
		ORDER BY timestamp ASC"
	// We use CURDATE() instead CURRENT_TIMESTAMP because we only want the date and not time related stuff (timekampf <- Max made me)
	);
});

app.MapGet("/yesterday", async () =>
{
	/// <summary>
	/// This endpoint returns all weather data from yesterday
	/// </summary>
	return await QueryParser.Parse(dbBuilder, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE cast(timestamp as date) = DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY)
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/week", async () =>
{
	/// <summary>
	/// This endpoint returns all weather data from the last week
	/// </summary>
	return await QueryParser.Parse(dbBuilder, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 WEEK) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/fortnight", async () =>
{
	/// <summary>
	/// This endpoint returns all weather data from the last two weeks (also called a fortnight)
	/// </summary>
	return await QueryParser.Parse(dbBuilder, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 WEEK) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/month", async () =>
{
	/// <summary>
	/// This endpoint returns all weather data from this month
	/// </summary>
	return await QueryParser.Parse(dbBuilder, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 MONTH) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/year", async () =>
{
	/// <summary>
	/// This endpoint returns all weather data from the last year
	/// </summary>
	return await QueryParser.Parse(dbBuilder, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 YEAR) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/on-date/{date}", async (DateTime date) =>
{
	/// <summary>
	/// This endpoint returns all weather data from a specific date
	/// </summary>
	/// <param name="date"></param>

	var formattedDate = date.ToString("yyyy-MM-dd");

	var query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE cast(timestamp as date) = '{0}' ORDER BY timestamp ASC", formattedDate);

	return await QueryParser.Parse(dbBuilder, query);
});

app.MapGet("/on-timestamp/{timestamp}", async (DateTime timestamp) =>
{

	/// <summary>
	/// This endpoint returns all weather data from a specific timestamp
	/// </summary>
	/// <param name="timestamp"></param>

	var formattedDate = timestamp.ToString("yyyy-MM-dd HH:mm:ss");

	var query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp = '{0}'", formattedDate);

	return await QueryParser.Parse(dbBuilder, query);
});

app.MapGet("/since/{timestamp}", async (DateTime timestamp) =>
{
	/// <summary>
	/// This endpoint returns all weather data since a specific timestamp
	/// </summary>
	/// <param name="timestamp"></param>
	
	var formattedDate = timestamp.ToString("yyyy-MM-dd HH:mm:ss");

	var query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE metadata.timestamp BETWEEN '{0}' AND CURRENT_TIMESTAMP ORDER BY timestamp ASC", formattedDate);

	return await QueryParser.Parse(dbBuilder, query);
});

app.MapGet("/devices", async () =>
{
	/// <summary>
	/// This endpoint returns all devices with their id
	/// This id is used for the /device endpoints
	/// </summary>
	return await QueryParser.GetDistinctStringColumn(dbBuilder, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");
});

app.MapGet("/gateways", async () =>
{
	/// <summary>
	/// This endpoint returns all gateways
	/// </summary>
	return await QueryParser.GetDistinctStringColumn(dbBuilder, @"SELECT DISTINCT gateway FROM metadata");
});

app.MapGet("/applications", async () =>
{
	/// <summary>
	/// This endpoint returns all the things network applications
	/// </summary>
	return await QueryParser.GetDistinctStringColumn(dbBuilder, @"SELECT DISTINCT application FROM metadata");
});

app.MapGet("/locations", async () =>
{
	/// <summary>
	/// This endpoint returns cities with sensors in them in the following format:
	/// city_name: {
	///		device_id : device_name
	/// }
	/// </summary>


	if (geocodeAPIKey == null)
	{
		var errorDict = new Dictionary<string, Dictionary<string, string>>();
		var errorMsgs = new Dictionary<string, string>();

		errorMsgs.Add("0", "Missing Google Maps Geocode API Key");

		errorDict.Add("error", errorMsgs);
		return errorDict;
	}

	// We use an id mapped to the response the SQL query from /devices would give us
	Dictionary<int, string> allDevices = await QueryParser.GetDistinctStringColumn(dbBuilder, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

	// Get all devices with their latitude and longitude

	/*
	   The returned data will look like this:

			{
				"device-name" : {
					"latitude": 23.23...,
					"longitude": 25.323,
				}
			}
	*/

	Dictionary<string, Dictionary<string, double>> deviceLocations = await QueryParser.GetDevicesLocations(
		dbBuilder, @"SELECT DISTINCT device, 
		latitude, longitude FROM positional
		INNER JOIN metadata ON metadata.id = positional.id
		ORDER BY metadata.device DESC");

	// This wil store our device: city entries
	Dictionary<string, Dictionary<string, string>> citiesWithDevices = new();

	int deviceIndex = 0;

	foreach (var deviceAndLocational in deviceLocations)
	{
		// Get location data from google maps api
		string apiURL = string.Format("https://maps.googleapis.com/maps/api/geocode/json?latlng={0},{1}&key={2}", deviceAndLocational.Value["latitude"], deviceAndLocational.Value["longitude"], geocodeAPIKey);

		HttpClient locationRequest = new();

		Stream responseBody = await locationRequest.GetStreamAsync(apiURL);

		GeocodeResponse.Root geoAPIResponse = await JsonSerializer.DeserializeAsync<GeocodeResponse.Root>(responseBody);

		string cityName = geoAPIResponse.results[0].address_components[3].short_name.Split(" ")[0];

		if (!citiesWithDevices.ContainsKey(cityName))
		{
			Dictionary<string, string> deviceInfo = new();
			deviceInfo.Add(Convert.ToString(deviceIndex), deviceAndLocational.Key);
			citiesWithDevices.Add(cityName, deviceInfo);
		} else
        {
			Dictionary<string, string> deviceList = citiesWithDevices[cityName];
			deviceList.Add(Convert.ToString(deviceIndex), deviceAndLocational.Key);
        }

		deviceIndex++;
	}

	return citiesWithDevices;

});

app.MapGet("/device/{deviceID}", async(int deviceID, DateTime? since) =>
{
	/// <summary>
	/// This endpoint returns device data from the last 24 hours by default
	/// </summary>
	/// <param name="timestamp"></param>

	// We use an id mapped to the response the SQL query from /devices would give us
	Dictionary<int, string> all_devices = await QueryParser.GetDistinctStringColumn(dbBuilder, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

	if (deviceID > all_devices.Count()) {
		// deviceID is out of bounds for our list, return an empty weatherpoint list

		return new List<WeatherPoint>();
	}

	var device = all_devices[deviceID];

	// since might be null, this makes sure if since is not passed, we take the current UTC time minus two hours
	DateTime assuredSince = since ?? DateTime.UtcNow.AddHours(-24);

	var formattedTimestamp = assuredSince.ToString("yyyy-MM-dd HH:mm:ss");

	var query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE device = '{0}' AND metadata.timestamp BETWEEN '{1}' AND CURRENT_TIMESTAMP ORDER BY timestamp ASC", device, formattedTimestamp);

	// return query;

	return await QueryParser.Parse(dbBuilder, query);
});

app.MapGet("/device/{deviceID}/location", async (int deviceID) =>
{

	if (geocodeAPIKey == null)
	{
		var errorDict = new Dictionary<string, string>();

		errorDict.Add("error", "Missing Google Maps Geocode API Key");
		return errorDict;
	}

	// We use an id mapped to the response the SQL query from /devices would give us
    Dictionary<int, string> allDevices = await QueryParser.GetDistinctStringColumn(dbBuilder, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

	if (deviceID > allDevices.Count() || allDevices.Count() == 0) {
		// deviceID is out of bounds for our list, return an empty weatherpoint list

		return new Dictionary<string, string>();
	}

	// Figure out which device name the user specified
	var device = allDevices[deviceID];
	
	var query = string.Format(@"SELECT DISTINCT device, 
		latitude, longitude FROM positional
		INNER JOIN metadata ON metadata.id = positional.id
		WHERE device = '{0}' ORDER BY metadata.timestamp DESC", device);

	Dictionary<string, Dictionary<string, double>> locations = await QueryParser.GetDevicesLocations(dbBuilder, query);

	Dictionary<string, double> deviceLocation = locations[device];

	string apiURL = string.Format("https://maps.googleapis.com/maps/api/geocode/json?latlng={0},{1}&key={2}", deviceLocation["latitude"], deviceLocation["longitude"], geocodeAPIKey);

	HttpClient locationRequest = new();

	Stream responseBody = await locationRequest.GetStreamAsync(apiURL);

	GeocodeResponse.Root geoAPIResponse = await JsonSerializer.DeserializeAsync<GeocodeResponse.Root>(responseBody);

	string cityName = geoAPIResponse.results[0].address_components[3].short_name.Split(" ")[0];

	Dictionary<string, string> returnData = new();

	returnData.Add(device, cityName);

	return returnData;
});

app.MapGet("/device/{deviceID}/average-temp", async (int deviceID, DateTime? since, DateTime? until) =>
{
	// We use an id mapped to the response from /devices here
	// You can get the id: device list from /devices, then pass the id for the device you want

	var all_devices = await QueryParser.GetDistinctStringColumn(dbBuilder, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

	if (deviceID > all_devices.Count())
	{
		// User requested from device that is out of bounds, just send an empty list back
		return new Dictionary<string, float>();
	}

	var device = all_devices[deviceID];

	// Return the average temp from the last hour
	DateTime sinceEnsured = since ?? DateTime.UtcNow.AddHours(-12);
	DateTime untilEnsured = until ?? DateTime.UtcNow;

	var sinceFormatted = sinceEnsured.ToString("yyyy-MM-dd HH:mm:ss");
	var untilFormatted = untilEnsured.ToString("yyyy-MM-dd HH:mm:ss");

	var query = string.Format(@"SELECT SUM(temperature)/COUNT(temperature) as avr FROM metadata
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		WHERE device = '{0}' 
		AND timestamp BETWEEN '{1}' AND '{2}'", device, sinceFormatted, untilFormatted);

	float averageTemp = await QueryParser.GetSingleFloatColumn(dbBuilder, query);

	Dictionary<string, float> returnData = new();

	returnData.Add(device, averageTemp);

	return returnData;
});

app.MapGet("/devices/locations", async () =>
{
    /// <summary>
    /// This endpoint returns cities with sensors in them in the following format:
    /// city_name: {
    ///        device_id : device_name
    /// }
    /// </summary>


    if (geocodeAPIKey == null)
    {
        var errorDict = new List<Dictionary<string, object>>();
        var errorMsgs = new Dictionary<string, string>();

        errorMsgs.Add("error", "missing Google Maps Geocode API Key");
        return errorDict;
    }

    // We use an id mapped to the response the SQL query from /devices would give us
    Dictionary<int, string> allDevices = await QueryParser.GetDistinctStringColumn(dbBuilder, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

    // Get all devices with their latitude and longitude

    /*
       The returned data will look like this:

		{
			"device-name" : {
				"latitude": 23.23...,
				"longitude": 25.323,
			}
		}
    */

    Dictionary<string, Dictionary<string, double>> deviceLocations = await QueryParser.GetDevicesLocations(
        dbBuilder, @"SELECT DISTINCT device, 
        latitude, longitude FROM positional
        INNER JOIN metadata ON metadata.id = positional.id
        ORDER BY metadata.device DESC");

    // This wil store our device: city entries
    List<Dictionary<string, object>> citiesWithDevices = new();

    /*
        [
            {
                "City" : "Enschede",
                "deviceID: py-saxion,
                "deviceIndex" : 1
            },
            ...
        ]
    */


    int deviceIndex = 0;

    foreach (var deviceAndLocational in deviceLocations)
    {
        // Get location data from google maps api
        string apiURL = string.Format("https://maps.googleapis.com/maps/api/geocode/json?latlng={0},{1}&key={2}", deviceAndLocational.Value["latitude"], deviceAndLocational.Value["longitude"], geocodeAPIKey);

        HttpClient locationRequest = new();

        Stream responseBody = await locationRequest.GetStreamAsync(apiURL);

        GeocodeResponse.Root geoAPIResponse = await JsonSerializer.DeserializeAsync<GeocodeResponse.Root>(responseBody);

        string cityName = geoAPIResponse.results[0].address_components[3].short_name.Split(" ")[0];

        Dictionary<string, object> cityAndDevice = new();
        cityAndDevice.Add("City", cityName);
        cityAndDevice.Add("deviceID", deviceAndLocational.Key);
        cityAndDevice.Add("deviceNumber", deviceIndex);

        citiesWithDevices.Add(cityAndDevice);

        // if (!citiesWithDevices.ContainsKey(cityName))
        // {
        //     Dictionary<string, string> deviceInfo = new();
        //     deviceInfo.Add(Convert.ToString(deviceIndex), deviceAndLocational.Key);
        //     citiesWithDevices.Add(cityName, deviceInfo);
        // } else
        // {
        //     Dictionary<string, string> deviceList = citiesWithDevices[cityName];
        //     deviceList.Add(Convert.ToString(deviceIndex), deviceAndLocational.Key);
        // }

        deviceIndex++;
    }

    return citiesWithDevices;

});

app.MapGet("/device/{deviceID}/latest", async (int deviceID) =>
{
	var all_devices = await QueryParser.GetDistinctStringColumn(dbBuilder, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

	if (deviceID > all_devices.Count())
	{
		// User requested from device that is out of bounds, just send an empty list back
		return new List<WeatherPoint>();
	}

	string device = all_devices[deviceID];

	string query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE device = '{0}' and metadata.id = (SELECT max(id) FROM metadata where device = '{1}')", device, device);

	return await QueryParser.Parse(dbBuilder, query);
});

app.MapGet("/latest", async () =>
{

	var latest = await QueryParser.Parse(dbBuilder, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE metadata.id = (SELECT max(id) FROM metadata)");

	return latest;
});

app.MapGet("/trans", async () =>
{

	var trans = await QueryParser.Parse(dbBuilder, 
		@"SELECT device, snr, rssi, MAX(metadata.id) as id, MAX(timestamp) as collected
		FROM transmissional_data, metadata 
		WHERE metadata.id = transmissional_data.id
		GROUP BY device");

	return trans;
});

app.MapGet("/battery", async () =>
{

	var bat = await QueryParser.Parse(dbBuilder, 
		@"SELECT device, battery_voltage, MAX(metadata.id) as id, MAX(timestamp) as collected
		FROM sensor_data, metadata 
		WHERE metadata.id = sensor_data.id
		GROUP BY device");

	return bat;
});

app.Run("http://0.0.0.0:5000");
