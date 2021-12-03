var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

var server = Environment.GetEnvironmentVariable("DB_ENDPOINT");
var userID = Environment.GetEnvironmentVariable("DB_USER");

var port_str = Environment.GetEnvironmentVariable("DB_PORT");

uint port = 3306; // Default mariaDB port

if (port_str != null)
{
	port = uint.Parse(port_str);
}

var password = Environment.GetEnvironmentVariable("DB_PASSWORD");
var database = Environment.GetEnvironmentVariable("DB_DB");

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
			WHERE metadata.timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 HOUR) AND CURRENT_TIMESTAMP"
	);
});

app.MapGet("/hour", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
			INNER JOIN positional ON metadata.id = positional.id
			INNER JOIN sensor_data ON metadata.id = sensor_data.id
			INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
			WHERE metadata.timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR) AND CURRENT_TIMESTAMP"
	);
});

app.MapGet("/today", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
			INNER JOIN positional ON metadata.id = positional.id
			INNER JOIN sensor_data ON metadata.id = sensor_data.id
			INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
			WHERE cast(metadata.timestamp as date) = CURDATE()" 
			// We use CURDATE() instead CURRENT_TIMESTAMP because we only want the date and not time related stuff (timekampf <- Max made me)
	);
});

app.MapGet("/yesterday", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
			INNER JOIN positional ON metadata.id = positional.id
			INNER JOIN sensor_data ON metadata.id = sensor_data.id
			INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
			WHERE cast(metadata.timestamp as date) = DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 DAY)"
	);
});

app.MapGet("/week", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
			INNER JOIN positional ON metadata.id = positional.id
			INNER JOIN sensor_data ON metadata.id = sensor_data.id
			INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
			WHERE metadata.timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 WEEK) AND CURRENT_TIMESTAMP"
	);
});

app.MapGet("/fortnight", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
			INNER JOIN positional ON metadata.id = positional.id
			INNER JOIN sensor_data ON metadata.id = sensor_data.id
			INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
			WHERE metadata.timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 2 WEEK) AND CURRENT_TIMESTAMP"
	);
});

app.MapGet("/month", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
			INNER JOIN positional ON metadata.id = positional.id
			INNER JOIN sensor_data ON metadata.id = sensor_data.id
			INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
			WHERE metadata.timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 MONTH) AND CURRENT_TIMESTAMP"
	);
});

app.MapGet("/year", async () =>
{
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
			INNER JOIN positional ON metadata.id = positional.id
			INNER JOIN sensor_data ON metadata.id = sensor_data.id
			INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
			WHERE metadata.timestamp BETWEEN DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 YEAR) AND CURRENT_TIMESTAMP"
	);
});

app.MapGet("/average-temp", async (DateTime since, DateTime until, int deviceID) =>
{
	// We use an id mapped to the response from /devices here
	// You can get the id: device list from /devices, then pass the id for the device you want

	var all_devices = await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");
	var device = all_devices.ElementAt(deviceID);

	var sinceFormatted = since.ToString("yyyy-MM-dd");
	var untilFormatted = until.ToString("yyyy-MM-dd");

	var query = string.Format(@"SELECT SUM(temperature)/COUNT(temperature) as avr FROM metadata
	INNER JOIN sensor_data ON metadata.id = sensor_data.id
	WHERE device = {0} 
	AND metadata.timestamp BETWEEN {1} AND {2}", device, sinceFormatted, untilFormatted);

	return await QueryParser.Parse(connection, query);
});

app.MapGet("/date", async (DateTime date) =>
{

	var formattedDate = date.ToString("yyyy-MM-dd");

	var query = string.Format(@"SELECT * FROM metadata
			INNER JOIN positional ON metadata.id = positional.id
			INNER JOIN sensor_data ON metadata.id = sensor_data.id
			INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
			WHERE cast(metadata.timestamp as date) = '{0}'", formattedDate);

	return await QueryParser.Parse(connection, query);
});

app.MapGet("/timestamp", async (DateTime timestamp) =>
{

	var formattedDate = timestamp.ToString("yyyy-MM-dd HH:mm:ss");

	var query = string.Format(@"SELECT * FROM metadata
			INNER JOIN positional ON metadata.id = positional.id
			INNER JOIN sensor_data ON metadata.id = sensor_data.id
			INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
			WHERE metadata.timestamp = '{0}'", formattedDate);

	return await QueryParser.Parse(connection, query);
});

app.MapGet("/since", async (DateTime timestamp) =>
{

	var formattedDate = timestamp.ToString("yyyy-MM-dd HH:mm:ss");

	var query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE metadata.timestamp BETWEEN '{0}' AND CURRENT_TIMESTAMP", formattedDate);

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

app.MapGet("/from-device", async (int deviceID) =>
{
	// We use an id mapped to the response from /devices here

	var all_devices = await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT device FROM metadata ORDER BY device DESC");
	var device = all_devices.ElementAt(deviceID);

	var query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE device = '{0}'", device);

	return await QueryParser.Parse(connection, query);
});

app.MapGet("/raw", async (DateTime since) =>
{
	/// This function should be used when the SQL query returns strings
	try
	{
		// Try opening
		await connection.OpenAsync();
	}
	catch (Exception)
	{
		// Connection is probably already open, so we should reset connection 
		await connection.ResetConnectionAsync();
	}

	MySqlConnector.MySqlDataReader reader;

	try
	{
		// Try running query
		using var command = connection.CreateCommand();

		command.CommandText = "SELECT * from raw_json";

		reader = await command.ExecuteReaderAsync();
	}
	catch (Exception ex)
	{
		await connection.CloseAsync();
		Console.WriteLine("Bad SQL query:");
		Console.WriteLine(ex.Message);
		return new List<JsonContent>();
	}

	List<JsonContent> raw_messages = new();
	
	while (await reader.ReadAsync())
    {
		raw_messages.Add(JsonContent.Create(reader.GetString("json")));
    }

	return raw_messages;
});


app.Run("http://localhost:5000");
