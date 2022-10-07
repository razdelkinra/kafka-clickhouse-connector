package com.project.clickhousekafkaconnector.dto;

import lombok.*;
import lombok.experimental.FieldDefaults;

@Getter
@Setter
@FieldDefaults(level = AccessLevel.PRIVATE)
@AllArgsConstructor
@NoArgsConstructor
public class ConnectDto {

    Long id;
    String topic;
    String table;
}
