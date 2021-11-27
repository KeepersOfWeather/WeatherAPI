
public class QueryParser
{
    public static async Task<IEnumerable<WeatherPoint>> Parse(MySqlConnector.MySqlConnection connection , string SQLQuery)
    {
		await connection.OpenAsync();

		using var command = connection.CreateCommand();

		command.CommandText = SQLQuery;

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

			// Check to see if altitude is missing, if it is we use our overload constructor which leaves altitude null

			var altitude_ordinal = reader.GetOrdinal("altitude");

			Positional positional;

			if (reader.IsDBNull(altitude_ordinal))
			{
				// We just leave altitude at our default null value
				positional = new(
					reader.GetDouble("latitude"),
					reader.GetDouble("longitude")
				);
			}
			else
			{ // Also set altitude
				positional = new(
					reader.GetDouble("latitude"),
					reader.GetDouble("longitude"),
					reader.GetDouble("altitude")
				);
			}

			SensorData sensorData;

			if (metadata.DeviceID.StartsWith("py"))
			{
				// PyComs only have temp, pressure and light in a logarithmic scale
				sensorData = new(
					reader.GetFloat("temperature"),
					reader.GetFloat("pressure"),
					reader.GetInt16("light_log_scale")
				);
			}
			else
			{
				// This is an LHT device, which supports a lot more data
				sensorData = new(
					reader.GetFloat("temperature"),
					reader.GetFloat("humidity"),
					reader.GetInt16("light_lux"),
					reader.GetInt16("battery_status"),
					reader.GetFloat("battery_voltage"),
					reader.GetString("work_mode")
					);
			}


			// Check if our SNR value is missing, some sensors don't send this?
			var snr_ordinal = reader.GetOrdinal("snr");
			TransmissionalData transmissionalData;

			if (reader.IsDBNull(snr_ordinal))
			{
				// It is missing, we use an overload constructer which won't initialise the SNR value
				transmissionalData = new(
					reader.GetInt16("rssi"),
					reader.GetInt16("spreading_factor"),
					reader.GetFloat("consumed_airtime"),
					reader.GetInt16("bandwidth"),
					reader.GetInt16("frequency")
				);

			}
			else
			{
				// It's there
				transmissionalData = new(
					reader.GetInt16("rssi"),
					reader.GetFloat("snr"),
					reader.GetInt16("spreading_factor"),
					reader.GetFloat("consumed_airtime"),
					reader.GetInt32("bandwidth"),
					reader.GetInt32("frequency")
				);
			}

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
	}
}
