package org.ebank.kafkastreams;

public class WeatherAverage {

    private double temperatureSum;
    private double humiditySum;
    private long count;

    public WeatherAverage() {
    }

    public void add(WeatherData weather) {
        temperatureSum += weather.getTemperature();
        humiditySum += weather.getHumidity();
        count++;
    }

    public double getAverageTemperature() {
        return temperatureSum / count;
    }

    public double getAverageHumidity() {
        return humiditySum / count;
    }
}