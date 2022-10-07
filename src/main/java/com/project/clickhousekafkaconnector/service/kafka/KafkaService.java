package com.project.clickhousekafkaconnector.service.kafka;

import com.project.clickhousekafkaconnector.configurtion.KafkaConfiguration;
import com.project.clickhousekafkaconnector.mapper.MessageConverter;
import com.project.clickhousekafkaconnector.repository.ClickHouseRepository;
import com.project.clickhousekafkaconnector.service.redis.RedisService;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Service
@FieldDefaults(level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
@Slf4j
public class KafkaService {

    public static String SUBSCRIBED = "Subscribed";
    public static int MAX_CONSUMER_COUNT = 10;

    final KafkaConfiguration kafkaConfiguration;
    final RedisService redisService;
    final ClickHouseRepository clickHouseRepository;
    final MessageConverter converter;

    /**
     * Ограничение на количество консьюмеров
     */
    private final AtomicInteger currentConsumerCount = new AtomicInteger();

    /**
     * Получение списка топиков
     */
    public Mono<Set<String>> getListTopic() {
        return Mono.just(getConsumer().listTopics().keySet()
                .stream().filter(s -> !s.startsWith("_")).collect(Collectors.toSet()));
    }

    /**
     * Подписка на топик и передача в Redis для накопления буфера для записи в ClickHouse
     */
    public Mono<String> subscribe(String topic, String table) {
        int consumerCount = currentConsumerCount.incrementAndGet();
        return consumerCount > MAX_CONSUMER_COUNT ? Mono.just("Max consumer count was reached") :
                KafkaReceiver.create(kafkaConfiguration.kafkaReactiveConsumer().subscription(Collections.singleton(topic)))
                        .receiveAutoAck()
                        .concatMap(r -> r)
                        .buffer(Duration.ofMillis(300))
                        .subscribeOn(Schedulers.parallel())
                        .doOnNext(records -> log.info("{} records has been read from topic {}", records.size(), topic))
                        .flatMap(records -> persist(topic, table, records))
                        .ignoreElements()
                        .map(receiverRecords -> SUBSCRIBED)
                        .doOnError(throwable -> log.error("Error subscribe to topic {} with cause {}", topic, throwable.getCause().getMessage()))
                        .switchIfEmpty(Mono.just(SUBSCRIBED));
    }

    private Mono<Long> persist(String topic, String table, List<ConsumerRecord<String, String>> records) {
        return redisService.addAll(topic, table, converter.getRecords(records))
                .onErrorResume(throwable -> clickHouseRepository.saveAsJson(converter.getRecords(records), table));
    }

    private KafkaConsumer<String, String> getConsumer() {
        return kafkaConfiguration.kafkaConsumer();
    }
}
