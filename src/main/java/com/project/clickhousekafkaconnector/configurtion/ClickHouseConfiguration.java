package com.project.clickhousekafkaconnector.configurtion;

import com.clickhouse.client.*;
import lombok.val;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ClickHouseConfiguration {

    @Value("${application.clickhouse.address}")
    private String ADDRESS;

    @Value("${application.clickhouse.port}")
    private int PORT;

    @Bean
    public ClickHouseRequest<?> clickHouseClient() {
        val server = ClickHouseNode.of(ADDRESS, ClickHouseProtocol.HTTP, PORT, null);
        val client = ClickHouseClient.newInstance(server.getProtocol());
        val connect = client.connect(server);
        connect.format(ClickHouseFormat.RowBinaryWithNamesAndTypes);
        return connect;
    }

}
