using System.Net;
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

var db_builder = new MySqlConnector.MySqlConnectionStringBuilder
{
    Server = server,
    UserID = userID,
    Port = port,
    Password = password,
    Database = database,
};

using var connection = new MySqlConnector.MySqlConnection(db_builder.ConnectionString);

app.MapGet("/", async () =>
{ 
	/// Retrieves weather info from last 2 hours
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/hour", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"	
	);
});

app.MapGet("/today", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
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
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE cast(timestamp as date) = DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY)
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/week", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 WEEK) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/fortnight", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 WEEK) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/month", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 MONTH) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/year", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 YEAR) AND CURRENT_TIMESTAMP
		ORDER BY timestamp ASC"
	);
});

app.MapGet("/on-date/{date}", async (DateTime date) =>
{

	var formattedDate = date.ToString("yyyy-MM-dd");

	var query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE cast(timestamp as date) = '{0}' ORDER BY timestamp ASC", formattedDate);

	return await QueryParser.Parse(connection, query);
});

app.MapGet("/on-timestamp/{timestamp}", async (DateTime timestamp) =>
{

	var formattedDate = timestamp.ToString("yyyy-MM-dd HH:mm:ss");

	var query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE timestamp = '{0}'", formattedDate);

	return await QueryParser.Parse(connection, query);
});

app.MapGet("/since/{timestamp}", async (DateTime timestamp) =>
{

	var formattedDate = timestamp.ToString("yyyy-MM-dd HH:mm:ss");

	var query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE metadata.timestamp BETWEEN '{0}' AND CURRENT_TIMESTAMP ORDER BY timestamp ASC", formattedDate);

	return await QueryParser.Parse(connection, query);
});

app.MapGet("/devices", async () =>
{
	return await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");
});

app.MapGet("/gateways", async () =>
{
	return await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT gateway FROM metadata");
});

app.MapGet("/applications", async () =>
{
	return await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT application FROM metadata");
});

app.MapGet("/device/{deviceID}", async(int deviceID, DateTime? since) =>
{
	// This endpoints returns device data from the last 24 hours by default

    // We use an id mapped to the response the SQL query from /devices would give us
    Dictionary<int, string> all_devices = await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

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

	return await QueryParser.Parse(connection, query);
});

app.MapGet("/device/{deviceID}/location", async (int deviceID) =>
{

	if (geocodeAPIKey == null)
    {
		return new Dictionary<string, string>();
	}

	// We use an id mapped to the response the SQL query from /devices would give us
    Dictionary<int, string> allDevices = await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

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

	Dictionary<string, Dictionary<string, double>> locations = await QueryParser.GetDevicesLocations(connection, query);

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

	var all_devices = await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

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

	float averageTemp = await QueryParser.GetSingleFloatColumn(connection, query);

	Dictionary<string, float> returnData = new();

	returnData.Add(device, averageTemp);

	return returnData;
});

app.MapGet("/devices/locations", async () =>
{

	if (geocodeAPIKey == null)
	{
		return new Dictionary<string, string>();
	}

	// We use an id mapped to the response the SQL query from /devices would give us
	Dictionary<int, string> allDevices = await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

	// Get all devices with their latitude and longitude
	Dictionary<string, Dictionary<string, double>> deviceLocations = await QueryParser.GetDevicesLocations(
		connection, @"SELECT DISTINCT device, 
		latitude, longitude FROM positional
		INNER JOIN metadata ON metadata.id = positional.id
		ORDER BY metadata.device DESC");

	// This wil store our device: city entries
	Dictionary<string, string> deviceCities = new();

	foreach (var deviceLocationEntry in deviceLocations)
    {
        string apiURL = string.Format("https://maps.googleapis.com/maps/api/geocode/json?latlng={0},{1}&key={2}", deviceLocationEntry.Value["latitude"], deviceLocationEntry.Value["longitude"], geocodeAPIKey);

        HttpClient locationRequest = new();

        Stream responseBody = await locationRequest.GetStreamAsync(apiURL);

        // object parsed = await JsonSerializer.DeserializeAsync<Dictionary<string, object>>(responseBody);

        GeocodeResponse.Root geoAPIResponse = await JsonSerializer.DeserializeAsync<GeocodeResponse.Root>(responseBody);

        string cityName = geoAPIResponse.results[0].address_components[3].short_name.Split(" ")[0];

		deviceCities.Add(deviceLocationEntry.Key, cityName);
    }

	return deviceCities;
});

app.MapGet("/device/{deviceID}/latest", async (int deviceID) =>
{
	var all_devices = await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");

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

	return await QueryParser.Parse(connection, query);
});

app.MapGet("/latest", async () =>
{

	var latest = await QueryParser.Parse(connection, @"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE metadata.id = (SELECT max(id) FROM metadata)");

	return latest;
});

app.Run("http://0.0.0.0:5000");
