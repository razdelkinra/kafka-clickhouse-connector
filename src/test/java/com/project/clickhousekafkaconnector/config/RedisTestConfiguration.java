package com.project.clickhousekafkaconnector.config;

import org.junit.Rule;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;


@TestConfiguration
public class RedisTestConfiguration implements InitializingBean {

    @Rule
    public static GenericContainer redisContainer = new GenericContainer(DockerImageName.parse("redis:5.0.3-alpine")
            .asCompatibleSubstituteFor("redis"))
            .withExposedPorts(6379);

    @Override
    public void afterPropertiesSet() {
        redisContainer.start();
    }

    @Bean
    @Primary
    public ReactiveRedisConnectionFactory reactiveRedisConnectionFactory() {
        return new LettuceConnectionFactory("localhost", redisContainer.getMappedPort(6379));
    }
}
