using System;

public class Metadata
{
    public DateTime TimeStamp { get; set; }
    public string DeviceID { get; set; }
    public string ApplicationID { get; set; }
    public string GatewayID { get; set; }

    public Metadata(DateTime timestamp, string deviceID, string applicationID, string gatewayID)
    {
        TimeStamp = timestamp;
        DeviceID = deviceID;
        ApplicationID = applicationID;
        GatewayID = gatewayID;
    }

    //Metadata(MySqlConnector.MySqlDataReader reader) {
    //    TimeStamp = reader.GetDateTime("timestamp");
    //    DeviceID = reader.GetString("device");
    //    ApplicationID = reader.GetString("application");
    //    GatewayID = reader.GetString("gateway");
    //}
}

public class Positional
{
    public double Latitude { get; set; }
    public double Longitude { get; set; }
    public double Altitude { get; set; }
    public string CityName { get; set; }

    public Positional(double latitude, double longitude, double altitude)
    {
        Latitude = latitude;
        Longitude = longitude;
        Altitude = altitude;
    }
}

public class SensorData
{
    public int LightLogscale { get; set; }
    public int LightLux { get; set; }
    public float Temperature { get; set; }
    public float Humidity { get; set; }
    public float Pressure { get; set; }
    public int BatteryStatus { get; set; }
    public float BatteryVoltage { get; set; }
    public string WordMode { get; set; }

    public SensorData(float temperature, float humidity, float pressure, 
        int lightLog, int lightLux, int batteryStatus, float batteryVoltage, 
        string wordMode)
    {
        LightLogscale = lightLog;
        LightLux = lightLux;
        Temperature = temperature;
            
        Humidity = humidity;
        Pressure = pressure;
        BatteryStatus = batteryStatus;
        BatteryVoltage = batteryVoltage;
        WordMode = wordMode;
    }
}

public class TransmissionalData
{
    public int Rssi { get; set; }
    public float Snr { get; set; }
    public int SpreadingFactor { get; set; }
    public float ConsumedAirtime { get; set; }
    public int Bandwidth { get; set; }
    public int Frequency { get; set; }

    public TransmissionalData(int rssi, float snr, int spreadingFactor, 
        float consumedAirtime, int bandwidth, int frequency)
    {
        Rssi = rssi;
        Snr = snr;
        SpreadingFactor = spreadingFactor;
        ConsumedAirtime = consumedAirtime;
        Bandwidth = bandwidth;
        Frequency = frequency;
    }
}

public class WeatherPoint
{

    public Metadata Metadata { get; set; }
    public Positional Positional { get; set; }
    public SensorData SensorData { get; set; }
    public TransmissionalData TransmissionalData { get; set; }

    public WeatherPoint(Metadata metadata, Positional positional, SensorData sensorData, TransmissionalData transmissionalData)
    {
        Metadata = metadata;
        Positional = positional;
        SensorData = sensorData;
        TransmissionalData = transmissionalData;
    }

}
