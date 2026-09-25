package org.ebank.kafkastreams;

public class WeatherData {

    private String station;
    private double temperature;
    private double humidity;

    public WeatherData(String station, double temperature, double humidity) {
        this.station = station;
        this.temperature = temperature;
        this.humidity = humidity;
    }

    public String getStation() {
        return station;
    }

    public double getTemperature() {
        return temperature;
    }

    public double getHumidity() {
        return humidity;
    }
}