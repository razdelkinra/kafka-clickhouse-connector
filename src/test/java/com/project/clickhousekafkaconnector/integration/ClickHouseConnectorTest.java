package com.project.clickhousekafkaconnector.integration;

import com.clickhouse.client.ClickHouseRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.clickhousekafkaconnector.KafkaClickhouseConnectorApplication;
import com.project.clickhousekafkaconnector.config.ClickHouseConfiguration;
import com.project.clickhousekafkaconnector.config.KafkaTestConfiguration;
import com.project.clickhousekafkaconnector.config.PostgresTestContainersConfiguration;
import com.project.clickhousekafkaconnector.config.RedisTestConfiguration;
import com.project.clickhousekafkaconnector.configurtion.KafkaConfiguration;
import com.project.clickhousekafkaconnector.dto.ConnectDto;
import com.project.clickhousekafkaconnector.dto.TestDto;
import com.project.clickhousekafkaconnector.mapper.MessageConverter;
import com.project.clickhousekafkaconnector.repository.ClickHouseRepository;
import com.project.clickhousekafkaconnector.repository.ConfigConnectRepository;
import com.project.clickhousekafkaconnector.service.ConfigConnectService;
import com.project.clickhousekafkaconnector.service.ConnectMapService;
import com.project.clickhousekafkaconnector.service.InitConfigurationService;
import com.project.clickhousekafkaconnector.service.clickhouse.ClickHouseDataLoader;
import com.project.clickhousekafkaconnector.service.clickhouse.ClickHouseService;
import com.project.clickhousekafkaconnector.service.kafka.KafkaService;
import com.project.clickhousekafkaconnector.service.redis.RedisService;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import reactor.kafka.receiver.ReceiverOptions;


@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {KafkaTestConfiguration.class, ClickHouseConfiguration.class, PostgresTestContainersConfiguration.class, RedisTestConfiguration.class})
@SpringBootTest(classes = KafkaClickhouseConnectorApplication.class)
public class ClickHouseConnectorTest {


    public static final String TEST_MESSAGE = "TEST_MESSAGE";
    public static final String SCADA_TABLE_TEST = "scada_table_test";
    public static final String SCADA_TOPIC_TEST = "scada_topic_test";
    public static final int RECORDS_NUMBER = 3;

    @Autowired
    KafkaProducer<String, String> kafkaProducer;
    @Autowired
    ConfigConnectRepository configConnectRepository;
    @Autowired
    ConnectMapService connectMapService;
    @Autowired
    ClickHouseRepository clickHouseRepository;
    @Autowired
    ClickHouseService clickHouseService;
    KafkaService kafkaService;
    @Autowired
    ConfigConnectService configConnectService;
    com.project.clickhousekafkaconnector.configurtion.KafkaConfiguration kafkaConfiguration;
    @Autowired
    RedisService redisService;
    @Autowired
    MessageConverter converter;
    @Autowired
    KafkaAdmin kafkaAdmin;
    @Autowired
    ReceiverOptions<String, String> kafkaReactiveConsumer;
    @Autowired
    org.apache.kafka.clients.consumer.KafkaConsumer KafkaConsumer;
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    ClickHouseDataLoader clickHouseDataLoader;
    @Autowired
    InitConfigurationService initConfigurationService;
    @Autowired
    ClickHouseRequest<?> clickHouseClient;
    @Autowired
    ObjectMapper objectMapper;

    @Before
    public void init() throws Exception {
        clickHouseClient.write().query("create table " + SCADA_TABLE_TEST + "(json String) engine = Memory").execute().get();

        kafkaConfiguration = Mockito.mock(KafkaConfiguration.class);
        Mockito.when(kafkaConfiguration.kafkaReactiveConsumer()).thenReturn(kafkaReactiveConsumer);
        Mockito.when(kafkaConfiguration.kafkaConsumer()).thenReturn(KafkaConsumer);

        kafkaService = new KafkaService(kafkaConfiguration, redisService, clickHouseRepository, converter);
        connectMapService = new ConnectMapService(clickHouseService, kafkaService, configConnectService);
        kafkaAdmin.createOrModifyTopics(new NewTopic(SCADA_TOPIC_TEST, 1, (short) 1));
    }


    @Test(timeout = 15000)
    public void shouldReturnInvalidTag() {

        ConnectDto connect = new ConnectDto();
        connect.setTable(SCADA_TABLE_TEST);
        connect.setTopic(SCADA_TOPIC_TEST);

        connectMapService.connectToChannel(connect).subscribe();

        sendMessageToKafka();

        clickHouseDataLoader.loadDataFromRedisToClickHouse().collectList().block();

        var result = clickHouseRepository.getJsonData(SCADA_TABLE_TEST).collectList().block();
        Assert.assertNotNull(result);
        Assert.assertEquals(RECORDS_NUMBER, result.size());
    }

    @SneakyThrows
    private void sendMessageToKafka() {
        String key = "key";
        String message = "message";
        TestDto testDto = new TestDto(message, true);
        ProducerRecord<String, String> keyMsgProducerRecord = new ProducerRecord<>(SCADA_TOPIC_TEST, key, objectMapper.writeValueAsString(testDto));
        for (int i = 0; i < RECORDS_NUMBER; i++) {
            kafkaTemplate.send(keyMsgProducerRecord).get();
        }
        Thread.sleep(3000);
    }

}
