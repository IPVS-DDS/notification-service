package de.unistuttgart.ipvs.dds;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import com.pengrad.telegrambot.TelegramBot;
import com.pengrad.telegrambot.model.request.ParseMode;
import com.pengrad.telegrambot.request.SendMessage;
import de.unistuttgart.ipvs.dds.avro.NotificationEvent;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import de.unistuttgart.ipvs.dds.avro.DeviceRegistration;
import de.unistuttgart.ipvs.dds.avro.SensorRegistration;
import de.unistuttgart.ipvs.dds.avro.TemperatureThresholdExceeded;

public class NotificationService {
    private final static Logger logger = LogManager.getLogger(NotificationService.class);

    public static void main(String[] args) {
        /* Telegram Bot token */
        if (args.length < 2) {
            logger.error("This application requires that you pass in a telegram bot token and a chat id");
            System.exit(1);
        }
        final String telegramToken = args[0];
        final String chatId = args[1];

        /* Zookeeper server URLs */
        final String bootstrapServers = args.length > 2 ? args[2] : "localhost:9092";
        /* URL of the Schema Registry */
        final String schemaRegistry = args.length > 3 ? args[3] : "http://localhost:8081";

        /* Client ID with which to register with Kafka */
        final String applicationId = "notification-service";

        /* The Kafka topic from which to read messages */
        final String inputTopic = "temperature-threshold-events";

        TelegramBot telegramBot = new TelegramBot(telegramToken);

        final Properties streamsProperties = new Properties();
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Get Serdes for every used data type.
        final SpecificAvroSerde<TemperatureThresholdExceeded> eventSerde = createSerde(schemaRegistry);
        final SpecificAvroSerde<SensorRegistration> sensorRegistrationSerde = createSerde(schemaRegistry);
        final SpecificAvroSerde<DeviceRegistration> deviceRegistrationSerde = createSerde(schemaRegistry);
        final SpecificAvroSerde<NotificationEvent> notificationSerde = createSerde(schemaRegistry);

        final KStreamBuilder builder = new KStreamBuilder();

        // Create a stream for reading TemperatureThresholdExceeded events.
        final KStream<String, TemperatureThresholdExceeded> events = builder.stream(
                Serdes.String(),
                eventSerde,
                inputTopic
        );
        // Create a KTable for looking up sensor metadata by sensor id.
        final KTable<String, SensorRegistration> sensorRegistry = builder.table(Serdes.String(), sensorRegistrationSerde, "sensor-registration");
        // Create a KTable for looking up device metadata by sensor id.
        final GlobalKTable<String, DeviceRegistration> deviceRegistry = builder.globalTable(Serdes.String(), deviceRegistrationSerde, "device-registration");

        // Create a factory for NotificationEvents.
        final NotificationEvent.Builder notificationBuilder = NotificationEvent.newBuilder();
        events
            // Initialise the notification event.
            .map((key, event) -> {
                final NotificationEvent notification = notificationBuilder.build();
                notification.setSensorId(key);
                if (event.getMaxTransgression().equals(event.getAverageTransgression())) {
                    // First transgression
                    notification.setMessage("the measured temperature now exceeds the threshold of "
                            + event.getTemperatureThreshold() + "C at " + event.getAverageTransgression() + "C");
                } else {
                    // Last transgression
                    notification.setMessage("the measured temperature no longer exceeds the threshold of "
                            + event.getTemperatureThreshold() + "C after " + event.getExceededForMs() + "ms. "
                            + "During this time, the average measurement was at " + event.getAverageTransgression() + "C "
                            + "with a maximum of " + event.getMaxTransgression() + "C");
                }
                return new KeyValue<>(key, notification);
            })
            // Correlate the event's sensorId with sensor metadata if possible.
            .leftJoin(sensorRegistry, (event, sensor) -> {
                if (sensor != null) {
                    event.setSensorName(sensor.getName());
                    event.setDeviceId(sensor.getDeviceId());
                }
                return event;
            }, Serdes.String(), notificationSerde)
            // Correlate the event's sensorId with the corresponding device metadata if possible.
            .join(deviceRegistry, (eventKey, event) -> event.getDeviceId(), (event, device) -> {
                event.setDeviceName(device.getName());
                event.setDeviceLocation(device.getLocation());
                return event;
            })
            // Create and send a Telegram message every time a NotificationEvent is received.
            .foreach((key, event) -> {
                StringBuilder message = new StringBuilder();
                message.append("Sensor ").append(event.getSensorName());
                message.append(" (").append(event.getSensorId()).append(") of ");
                message.append("device ").append(event.getDeviceName());
                message.append(" (").append(event.getDeviceId()).append(") ");
                if (event.getDeviceLocation() != null && !event.getDeviceLocation().equals("")) {
                    message.append("located at ").append(event.getDeviceLocation()).append(" ");
                }
                message.append("reports that ");
                message.append(event.getMessage());
                message.append('.');

                SendMessage req = new SendMessage(chatId, message.toString())
                        .parseMode(ParseMode.HTML)
                        .disableNotification(true);
                telegramBot.execute(req);
            });

        final KafkaStreams streams = new KafkaStreams(builder, streamsProperties);
        streams.start();
    }

    /**
     * Creates a serialiser/deserialiser for the given type, registering the Avro schema with the schema registry.
     *
     * @param schemaRegistryUrl the schema registry to register the schema with
     * @param <T>               the type for which to create the serialiser/deserialiser
     * @return                  the matching serialiser/deserialiser
     */
    private static <T extends SpecificRecord> SpecificAvroSerde<T> createSerde(final String schemaRegistryUrl) {
        final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schemaRegistryUrl
        );
        serde.configure(serdeConfig, false);
        return serde;
    }
}
