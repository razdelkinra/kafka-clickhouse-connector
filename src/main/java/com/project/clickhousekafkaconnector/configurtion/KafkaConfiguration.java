package com.project.clickhousekafkaconnector.configurtion;

import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

@Component
public class KafkaConfiguration {

    @Value("${application.kafka.bootstrap-servers}")
    private String BOOTSTRAP_SERVERS_CONFIG;

    @Value("${application.kafka.group-id}")
    private String GROUP_ID;

    @Value("${application.kafka.application-id}")
    private String APPLICATION_ID;

    @Value("${application.kafka.schema-registry-url}")
    private String SCHEMA_REGISTRY;

    public KafkaConsumer<String, String> kafkaConsumer() {
        val props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.setProperty(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY);
        return new KafkaConsumer<>(props);
    }

    public ReceiverOptions<String, String> kafkaReactiveConsumer() {
        val props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, APPLICATION_ID + new Random().nextInt());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return ReceiverOptions.create(props);
    }

}
