package com.project.clickhousekafkaconnector.service.redis;

import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Service
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Component
public class KeyService {

    static final String SEPARATOR = ":";
    Map<String, String> keys = new ConcurrentHashMap<>();

    /**
     * Генерация ключа для записи в Redis
     */
    public String getKey(String topic, String table) {
        keys.putIfAbsent(topic, getTopicFullName(topic, table));
        return keys.get(topic);
    }

    /**
     * Список ключей для всех топиков
     */
    public List<String> getKeys() {
        return new ArrayList<>(keys.values());
    }

    /**
     * Обновить ключ для топика
     */
    public String updateKey(String key) {
        val split = key.split(SEPARATOR);
        if (split.length > 2) {
            val topicName = String.join(SEPARATOR, List.of(split).subList(0, split.length - 2));
            val tableName = String.join(SEPARATOR, List.of(split).subList(1, split.length - 1));
            if (StringUtils.isNotBlank(topicName)) {
                keys.put(topicName, getTopicFullName(topicName, tableName));
                return keys.get(topicName);
            }
        }
        return "";
    }

    public String getTable(String key) {
        val split = key.split(SEPARATOR);
        return split.length > 1 ? key.split(SEPARATOR)[1] : "";
    }

    public Set<String> getActiveTable() {
        return keys.keySet();
    }

    private String getTopicFullName(String topic, String table) {
        return topic.replaceAll("[^a-zA-Z0-9]", "") + SEPARATOR + table + SEPARATOR + (new Random().nextInt() & Integer.MAX_VALUE);
    }
}