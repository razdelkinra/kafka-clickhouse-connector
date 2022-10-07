package com.project.clickhousekafkaconnector.service.clickhouse;

import com.project.clickhousekafkaconnector.repository.ClickHouseRepository;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

@Service
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@AllArgsConstructor
public class ClickHouseService {

    ClickHouseRepository clickHouseRepository;

    public Mono<Long> save(List<String> data, String table) {
        return clickHouseRepository.saveAsJson(data, table);
    }

    public Mono<Set<String>> getTables() {
        return clickHouseRepository.getTables();
    }
}
