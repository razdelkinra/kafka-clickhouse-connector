package com.project.clickhousekafkaconnector.controller;

import com.project.clickhousekafkaconnector.dto.ConnectDto;
import com.project.clickhousekafkaconnector.dto.ConnectMapDto;
import com.project.clickhousekafkaconnector.service.ConnectMapService;
import com.project.clickhousekafkaconnector.service.kafka.KafkaService;
import lombok.AllArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1")
@AllArgsConstructor
public class ConnectController {

    KafkaService kafkaService;
    ConnectMapService connectMapService;

    @GetMapping(value = "/topic")
    Mono<ResponseEntity<ConnectMapDto>> getConnectionData() {
        return connectMapService.getConnectionData().map(configData -> ResponseEntity.ok().body(configData));
    }

    @PostMapping(value = "/channel")
    Mono<ResponseEntity<String>> createChannel(@RequestBody ConnectDto connectDto) {
        return connectMapService.createChannel(connectDto).ignoreElements().map(configData -> ResponseEntity.ok().body(configData));
    }

}
