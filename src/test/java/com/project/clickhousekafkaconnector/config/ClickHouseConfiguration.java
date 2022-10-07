package com.project.clickhousekafkaconnector.config;

import com.clickhouse.client.*;
import lombok.val;
import org.junit.Rule;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.utility.DockerImageName;


public class ClickHouseConfiguration implements InitializingBean {

    @Rule
    public ClickHouseContainer clickHouseContainer = new ClickHouseContainer(DockerImageName.parse("yandex/clickhouse-server:latest")
            .asCompatibleSubstituteFor("yandex/clickhouse-server"));

    @Bean
    @Primary
    public ClickHouseRequest<?> clickHouseClient() {
        val server = ClickHouseNode.of(clickHouseContainer.getHost(), ClickHouseProtocol.HTTP, clickHouseContainer.getMappedPort(8123), null);
        val client = ClickHouseClient.newInstance(server.getProtocol());
        val connect = client.connect(server);
        connect.format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        return connect;
    }

    @Override
    public void afterPropertiesSet() {
        clickHouseContainer.start();
    }
}
