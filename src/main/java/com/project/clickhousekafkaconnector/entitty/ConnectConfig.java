package com.project.clickhousekafkaconnector.entitty;

import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@FieldDefaults(level = AccessLevel.PRIVATE)
@Table(value = "kafka_ch_connect_config")
public class ConnectConfig {
    @Id
    Integer id;
    @Column("topic_name")
    String topic;
    @Column("table_name")
    String table;
    @Column("is_enable")
    Boolean isEnable;
}
