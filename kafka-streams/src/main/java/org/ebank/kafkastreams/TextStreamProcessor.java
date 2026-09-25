package org.ebank.kafkastreams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;



@Configuration
@EnableKafkaStreams
public class TextStreamProcessor {

    @Bean
    public KStream<String, String> process(StreamsBuilder builder) {

        KStream<String, String> cleanedStream = builder
                .<String, String>stream("text-input")
                .mapValues(value -> value.trim().replaceAll("\\s+", " ").toUpperCase());

        //cleanedStream.to("text-clean");

        KStream<String, String> validStream = cleanedStream.filter(
                (key, value) ->
                        !value.isEmpty()
                                && value.length() <= 100
                                && !value.contains("HACK")
                                && !value.contains("SPAM")
                                && !value.contains("XXX")
                     );
        validStream.to("text-clean");

        //gérer message invalides
        KStream<String, String> invalidStream = cleanedStream.filter(
                (key, value) ->
                        value.isEmpty()
                                || value.length() > 100
                                || value.contains("HACK")
                                || value.contains("SPAM")
                                || value.contains("XXX")
        );

        invalidStream.to("text-dead-letter");
        return cleanedStream;
    }
}
