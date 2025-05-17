package com.example.integration.controller;

import com.example.integration.service.MqttToKafkaService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final MqttToKafkaService mqttToKafkaService;
    // 기존 메트릭과 별개로, 실시간 처리량만 뽑아주는 엔드포인트
    @GetMapping
    public Map<String, Object> realtime() {
        return mqttToKafkaService.getRealtimeMetrics();
    }
}