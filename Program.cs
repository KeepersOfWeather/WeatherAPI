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
await connection.OpenAsync();

using var command = connection.CreateCommand();

    command.CommandText = @"SELECT * FROM metadata
INNER JOIN positional ON metadata.id = positional.id
INNER JOIN sensor_data ON metadata.id = sensor_data.id
INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
WHERE cast(metadata.timestamp as date) = '2021-11-27'";

    //command.CommandText = @"SELECT * FROM metadata";

    using var reader = await command.ExecuteReaderAsync();

List<WeatherPoint> weatherPoints = new();

while (reader.Read())
{
	Metadata metadata = new(
		reader.GetDateTime("timestamp"),
		reader.GetString("device"),
		reader.GetString("application"),
		reader.GetString("gateway")
	);

	Positional positional = new(
		reader.GetDouble("latitude"),
		reader.GetDouble("longitude"),
		reader.GetDouble("altitude")
		);

	SensorData sensorData = new(
		reader.GetFloat("temperature"),
		reader.GetFloat("humidity"),
		reader.GetFloat("pressure"),
		reader.GetInt16("light_lux"),
		reader.GetInt16("light_log_scale"),
		reader.GetInt16("battery_status"),
		reader.GetFloat("battery_voltage"),
		reader.GetString("work_mode")

		);

	TransmissionalData transmissionalData = new(
		reader.GetInt16("rssi"),
		reader.GetFloat("snr"),
		reader.GetInt16("spreading_factor"),
		reader.GetFloat("consumed_airtime"),
		reader.GetInt16("bandwidth"),
		reader.GetInt16("frequency")
		);

	WeatherPoint weatherPoint = new(
		metadata,
		positional,
		sensorData,
		transmissionalData
		);

		weatherPoints.Add(weatherPoint);
	}



    await connection.CloseAsync();

	return weatherPoints;
});

app.Run();
