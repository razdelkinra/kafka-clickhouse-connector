package com.project.clickhousekafkaconnector.service.redis;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;


@Service
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@AllArgsConstructor
public class RedisService {

    ReactiveStringRedisTemplate redisTemplate;
    KeyService keyService;

    public Mono<Long> addAll(String key, String table, List<String> records) {
        return redisTemplate.opsForList().rightPushAll(keyService.getKey(key, table), records);
    }

    public Flux<String> getKeys() {
        return Flux.fromIterable(keyService.getKeys());
    }

    public Mono<String> updateKey(String key) {
        return Mono.just(keyService.updateKey(key));
    }

    public Mono<Long> delete(String key) {
        return redisTemplate.delete(key);
    }

    public Flux<String> findAll(String key) {
        return redisTemplate.opsForList().size(key)
                .flatMapMany(size -> size == null ? Flux.empty() : redisTemplate.opsForList().range(key, 0, size));
    }
}
