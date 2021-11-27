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
	return await QueryParser.Parse(connection, @"SELECT * FROM metadata
			INNER JOIN positional ON metadata.id = positional.id
			INNER JOIN sensor_data ON metadata.id = sensor_data.id
			INNER JOIN transmissional_data ON metadata.id = transmissional_data.id
			WHERE cast(metadata.timestamp as date) = '2021-11-27'"
	);
});

app.Run();
