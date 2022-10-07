package com.project.clickhousekafkaconnector.repository;

import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Repository
@FieldDefaults(level = AccessLevel.PRIVATE)
@RequiredArgsConstructor
public class ClickHouseRepository {

    public static String JSON_COLUMN_NAME = "json";
    final ClickHouseRequest clickHouseClient;

    public Mono<Long> saveAsJson(List<String> data, String table) {
        return Objects.isNull(table) ?
                Mono.just(0L) :
                Mono.fromFuture(clickHouseClient
                        .write()
                        .query("INSERT INTO " + table + " (" + JSON_COLUMN_NAME + ") FORMAT JSONAsString " + String.join(",", data))
                        .execute())
                        .map(clickHouseResponse -> Long.valueOf(data.size()));
    }

    public Flux<String> getJsonData(String table) {
        return Mono.fromFuture(clickHouseClient
                .write()
                .query("Select * from " + table)
                .execute())
                .flatMapIterable(ClickHouseResponse::records)
                .map(clickHouseValues -> clickHouseValues.getValue(JSON_COLUMN_NAME))
                .map(Object::toString);
    }

    public Mono<Set<String>> getTables() {
        return Mono.fromFuture(clickHouseClient.write().query("select * from system.tables").execute())
                .map(clickHouseResponse -> StreamSupport.stream(clickHouseResponse.records().spliterator(), false)
                        .filter(clickHouseValues -> !clickHouseValues.getValue("database").asString().equalsIgnoreCase("system"))
                        .map(res -> res.getValue("name").asString())
                        .collect(Collectors.toSet()));

    }
}
