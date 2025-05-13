package com.example.integration.controller;

import com.example.integration.service.LoadTestService;
import com.example.integration.service.MqttToKafkaService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final LoadTestService loadTestService;
    private final MqttToKafkaService metricService;

    @GetMapping
    public Map<String, Object> all() {
        LinkedHashMap<String, Object> allMetrics = new LinkedHashMap<>();
        allMetrics.put("mqtt", mqtt());
        allMetrics.put("rabbitmq", rabbitmq());
        allMetrics.put("kafka", kafka());
        return allMetrics;
    }

    @GetMapping("/mqtt")
    public Map<String, Object> mqtt() {
        LinkedHashMap<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("receivedCount", metricService.getMqttReceivedCount());
        metrics.put("receivedBytes", metricService.getMqttReceivedBytes());
        metrics.put("receiveRatePerSec", metricService.getMqttReceiveRatePerSec());
        metrics.put("sentCount", loadTestService.getSentCount());
        metrics.put("sentBytes", loadTestService.getSentBytes());
        metrics.put("sendRatePerSec", loadTestService.getSendRatePerSec());
        return metrics;
    }

    @GetMapping("/rabbitmq")
    public Map<String, Object> rabbitmq() {
        LinkedHashMap<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("receivedCount", metricService.getRabbitReceivedCount());
        metrics.put("receivedBytes", metricService.getRabbitReceivedBytes());
        metrics.put("receiveRatePerSec", metricService.getRabbitReceiveRatePerSec());
        return metrics;
    }

    @GetMapping("/kafka")
    public Map<String, Object> kafka() {
        LinkedHashMap<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("sentCount", metricService.getKafkaSentCount());
        metrics.put("sentBytes", metricService.getKafkaSentBytes());
        metrics.put("sendRatePerSec", metricService.getKafkaSendRatePerSec());
        return metrics;
    }
}
