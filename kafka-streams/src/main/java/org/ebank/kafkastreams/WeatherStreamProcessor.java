package org.ebank.kafkastreams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WeatherStreamProcessor {
    @Bean
    public KStream<String, String> weatherStream(StreamsBuilder builder) {

        KStream<String, String> weatherStream =
                builder.<String, String>stream("weather-data");

        KStream<String, WeatherData> parsedStream = weatherStream.mapValues(value -> {
            String[] parts = value.split(",");

            String station = parts[0].trim();
            double temperature = Double.parseDouble(parts[1].trim());
            double humidity = Double.parseDouble(parts[2].trim());

            return new WeatherData(station, temperature, humidity);
        });

        KStream<String, WeatherData> filteredStream =
                parsedStream.filter((key, weather) ->
                        weather.getTemperature() > 30
                );
        KStream<String, WeatherData> fahrenheitStream =
                filteredStream.mapValues(weather -> {

                    double fahrenheit =
                            (weather.getTemperature() * 9 / 5) + 32;

                    return new WeatherData(
                            weather.getStation(),
                            fahrenheit,
                            weather.getHumidity()
                    );
                });
        KGroupedStream<String, WeatherData> groupedStream =
                fahrenheitStream
                        .selectKey((key, weather) -> weather.getStation())
                        .groupByKey();

        KTable<String, WeatherAverage> averagesTable =
                groupedStream.aggregate(
                        WeatherAverage::new,
                        (station, weather, average) -> {
                            average.add(weather);
                            return average;
                        }
                );
        averagesTable.toStream()
                .mapValues(average ->
                        "Temperature moyenne = " + average.getAverageTemperature()
                                + " F, Humidite moyenne = "
                                + average.getAverageHumidity() + " %"
                )
                .to("station-averages");
        return weatherStream;
    }
}
