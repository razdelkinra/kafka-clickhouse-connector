package com.project.clickhousekafkaconnector.configurtion;

import io.lettuce.core.ReadFrom;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.Arrays;

@Configuration
@FieldDefaults(level = AccessLevel.PRIVATE)
public class InMemoryDatabaseConfiguration {

    @Value("${application.in-memory.address}")
    String[] ADDRESS;

    @Bean
    public ReactiveRedisConnectionFactory reactiveRedisConnectionFactory() {
        RedisClusterConfiguration clusterConfig = new RedisClusterConfiguration(Arrays.asList(ADDRESS));
        clusterConfig.setMaxRedirects(3);
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder().readFrom(ReadFrom.REPLICA_PREFERRED).build();
        return new LettuceConnectionFactory(clusterConfig, clientConfig);
    }

}
