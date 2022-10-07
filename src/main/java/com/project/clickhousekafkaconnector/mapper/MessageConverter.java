package com.project.clickhousekafkaconnector.mapper;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@AllArgsConstructor
public class MessageConverter {

    public List<String> getRecords(List<ConsumerRecord<String, String>> records) {
        return records.stream().map(ConsumerRecord::value).collect(Collectors.toList());
    }

}
