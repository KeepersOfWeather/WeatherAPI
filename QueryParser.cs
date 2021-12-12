using MySqlConnector;

public class QueryParser
{
    public static async Task<IEnumerable<WeatherPoint>> Parse(MySqlConnector.MySqlConnection connection , string SQLQuery)
    {
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

		MySqlDataReader reader;

		try
		{
			// Try running query
			using var command = connection.CreateCommand();

			command.CommandText = SQLQuery;

			reader = await command.ExecuteReaderAsync();
		} catch (Exception ex)
        {
			await connection.CloseAsync();
			Console.WriteLine("Bad SQL query:");
			Console.WriteLine(ex.Message);
			return new List<WeatherPoint>();
		}

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

			if (metadata.DeviceID.Contains("py"))
			{
				// PyComs only have temp, pressure and light in a logarithmic scale
				sensorData = new(
					reader.GetFloat("temperature"),
					reader.GetFloat("pressure"),
					reader.GetInt32("light_log_scale")
				);
			}
			else if (metadata.DeviceID.Contains("lht"))
			{
				// This is an LHT device, which supports a lot more data
				sensorData = new(
					reader.GetFloat("temperature"),
					reader.GetFloat("humidity"),
					reader.GetInt32("light_lux"),
					reader.GetInt32("battery_status"),
					reader.GetFloat("battery_voltage"),
					reader.GetString("work_mode")
					);
			} else
            {
				Console.Error.WriteLine("Unknown device type in Database: ");
				return new List<WeatherPoint>();
            }


			// Check if our SNR value is missing, some sensors don't send this?
			var snr_ordinal = reader.GetOrdinal("snr");
			TransmissionalData transmissionalData;

			if (reader.IsDBNull(snr_ordinal))
			{
				// It is missing, we use an overload constructer which won't initialise the SNR value
				transmissionalData = new(
					reader.GetInt32("rssi"),
					reader.GetInt32("spreading_factor"),
					reader.GetFloat("consumed_airtime"),
					reader.GetInt32("bandwidth"),
					reader.GetInt32("frequency")
				);

			}
			else
			{
				// It's there
				transmissionalData = new(
					reader.GetInt32("rssi"),
					reader.GetFloat("snr"),
					reader.GetInt32("spreading_factor"),
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

	public async static Task<Dictionary<int, string>> GetDistinctStringColumn(MySqlConnection connection, string SQLQuery)
    {
		/// This function should be used when the SQL query returns strings
		try
		{
			// Try opening
			await connection.OpenAsync();
		}
		catch (Exception)
		{
			if (connection.State == System.Data.ConnectionState.Open)
			{
				// Connection is probably already open, so we should reset connection 
				await connection.ResetConnectionAsync();
			} else
            {
				throw;
            }
		}

        MySqlDataReader reader;

		try
		{
			// Try running query
			using var command = connection.CreateCommand();

			command.CommandText = SQLQuery;

			reader = await command.ExecuteReaderAsync();
		}
		catch (Exception ex)
		{
			await connection.CloseAsync();
			Console.WriteLine("Bad SQL query:");
			Console.WriteLine(ex.Message);
			return new Dictionary<int, string>();
		}

		List<string> values = new();

		while (reader.Read())
		{
			try
			{
				values.Add(reader.GetString(0));
			} 
			catch (Exception ex)
            {
				Console.WriteLine(ex.Message);
            }
		}

		await connection.CloseAsync();

		Dictionary<int, string> enum_response = new();

		foreach (var it in values.Select((Value, Index) => new { Value, Index }))
		{
			enum_response.Add(it.Index, it.Value);
		}

		return enum_response;
	}

	public async static Task<float> GetSingleFloatColumn(MySqlConnection connection, string SQLQuery)
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

		MySqlDataReader reader;

		try
		{
			// Try running query
			using var command = connection.CreateCommand();

			command.CommandText = SQLQuery;

			reader = await command.ExecuteReaderAsync();
		}
		catch (Exception ex)
		{
			await connection.CloseAsync();
			Console.WriteLine("Bad SQL query:");
			Console.WriteLine(ex.Message);
			return new float();
		}

		float value = new();

		while (reader.Read())
		{
			try
			{
				value = reader.GetFloat(0);
			}
			catch (Exception ex)
			{
				Console.WriteLine(ex.Message);
			}
		}

		await connection.CloseAsync();

		return value;
	}

	public async static Task<Dictionary<string, Dictionary<string, double>>> GetDevicesLocations(MySqlConnection connection, string SQLQuery) {
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

		MySqlDataReader reader;

		try
		{
			// Try running query
			using var command = connection.CreateCommand();

			command.CommandText = SQLQuery;

			reader = await command.ExecuteReaderAsync();
		}
		catch (Exception ex)
		{
			await connection.CloseAsync();
			Console.WriteLine("Bad SQL query:");
			Console.WriteLine(ex.Message);
			return new Dictionary<string, Dictionary<string, double>>();
		}

		Dictionary<string, Dictionary<string, double>> locations = new();		

		/*
		 
			The returned data will look like this:

			{
				"device-name" : {
					"latitude": 23.23...,
					"longitude": 25.323,
				}
			}
		 
		 */

		while (reader.Read())
		{

			string device = reader.GetString("device");
			double latitude = reader.GetDouble("latitude");
			double longitude = reader.GetDouble("longitude");

			Dictionary<string, double> coordinates = new();

			coordinates.Add("latitude", latitude);
			coordinates.Add("longitude", longitude);
			try
			{
				locations.Add(device, coordinates);
			} catch (Exception ex)
            {
				Console.WriteLine(ex.Message);
				Console.WriteLine(string.Format("Locations: {0}", locations));
            }
		}

		// Make sure to close connection, otherwise next request will fail when opening 
		await connection.CloseAsync();

		return locations;
	}

}
