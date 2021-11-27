var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

var db_builder = new MySqlConnector.MySqlConnectionStringBuilder
{
	Server = "weather.camiel.pw",
	UserID = "root",
	Password = "weer123",
	Database = "mqtt",
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

app.MapGet("/average-temp", async (DateTime since, DateTime until, string deviceID) =>
{
	var sinceFormatted = since.ToString("yyyy-MM-dd");
	var untilFormatted = until.ToString("yyyy-MM-dd");

	var query = string.Format(@"SELECT SUM(temperature)/COUNT(temperature) as avr FROM metadata
	INNER JOIN sensor_data ON metadata.id = sensor_data.id
	WHERE device = {0} 
	AND metadata.timestamp BETWEEN {1} AND {2}", deviceID, sinceFormatted, untilFormatted);

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
	return await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT device FROM metadata");
});

app.MapGet("/gateways", async () =>
{
	return await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT gateway FROM metadata");
});

app.MapGet("/applications", async () =>
{
	return await QueryParser.GetDistinctStringColumn(connection, @"SELECT DISTINCT application FROM metadata");
});

app.MapGet("/from-device", async (string id) =>
{
	var query = string.Format(@"SELECT * FROM metadata
		INNER JOIN positional ON metadata.id = positional.id
		INNER JOIN sensor_data ON metadata.id = sensor_data.id
		INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
		WHERE device = '{0}'", id);

	return await QueryParser.Parse(connection, query);
});

// TODO: filter endpoint

app.Run();