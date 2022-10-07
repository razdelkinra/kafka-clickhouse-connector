package com.project.clickhousekafkaconnector.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import lombok.val;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Scope;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;


@TestConfiguration
public class KafkaTestConfiguration implements InitializingBean {

    public static final String KAFKA_VERSION = "6.2.0";
    public static final DockerImageName KAFKA_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:" + KAFKA_VERSION);
    public static final DockerImageName SCHEMA_REGISTRY_IMAGE = DockerImageName.parse("confluentinc/cp-schema-registry:" + KAFKA_VERSION);
    public static final int SCHEMA_REGISTRY_PORT = 8081;
    public static Network network = Network.newNetwork();

    public static KafkaContainer kafka;
    public static GenericContainer kafkaSchemaRegistry;

    public static String getSchemaRegistryHost() {
        return "http://" + kafkaSchemaRegistry.getHost() + ":" + kafkaSchemaRegistry.getMappedPort(SCHEMA_REGISTRY_PORT);
    }

    @Bean
    @Primary
    public KafkaConsumer<String, String> kafkaConsumer() {
        val props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "GROUP_ID");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        props.setProperty(SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryHost());
        return new KafkaConsumer<>(props);
    }

    @Bean
    @Primary
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate(producerFactory());
    }

    @Bean
    public KafkaProducer<String, String> KafkaProducer() {
        return new KafkaProducer(kafkaTestProducerProperties());
    }

    @Bean
    @Primary
    public KafkaAdmin kafkaAdmin() {
        return new KafkaAdmin(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
    }

    @Bean
    @Scope(value = "prototype")
    public SenderOptions<Object, Object> kafkaReactiveProducer() {
        val producerConfig = new HashMap<String, Object>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerConfig.put(ProducerConfig.CLIENT_ID_CONFIG, "test_app" + new Random().nextInt());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "0");
        return SenderOptions.create(producerConfig);
    }

    @Bean
    public ReceiverOptions<String, String> kafkaReactiveConsumer() {
        val props = new HashMap<String, Object>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "APPLICATION_ID" + new Random().nextInt());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "GROUP_ID");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryHost());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return ReceiverOptions.create(props);
    }

    @Bean
    @Primary
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryHost());
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public Properties kafkaTestConsumerProperties() {
        val props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_group_id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryHost());
        return props;
    }

    @Bean
    public Properties kafkaTestProducerProperties() {
        val props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "APPLICATION_ID" + new Random().nextInt());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(SCHEMA_REGISTRY_URL_CONFIG, getSchemaRegistryHost());
        props.put(ProducerConfig.ACKS_CONFIG, "0");
        return props;
    }

    @Override
    public void afterPropertiesSet() {
        kafka = new KafkaContainer(KAFKA_IMAGE
                .asCompatibleSubstituteFor("confluentinc/cp-kafka"))
                .withEnv("KAFKA_HOST_NAME", "kafka")
                .withNetworkAliases("kafka")
                .withNetwork(network);

        kafka.start();

        kafkaSchemaRegistry = new GenericContainer(SCHEMA_REGISTRY_IMAGE
                .asCompatibleSubstituteFor("confluentinc/cp-schema-registry"))
                .withNetwork(network)
                .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
                .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
                .withExposedPorts(SCHEMA_REGISTRY_PORT);

        kafkaSchemaRegistry.start();
    }

}
