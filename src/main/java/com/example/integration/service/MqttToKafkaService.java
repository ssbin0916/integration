package com.example.integration.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class MqttToKafkaService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.topic}")
    private String kafkaTopic;

    // metrics
    private final AtomicLong mqttReceivedCount = new AtomicLong();
    private final AtomicLong mqttReceivedBytes = new AtomicLong();
    private final AtomicLong rabbitReceivedCount = new AtomicLong();
    private final AtomicLong rabbitReceivedBytes = new AtomicLong();
    private final AtomicLong kafkaSentCount = new AtomicLong();
    private final AtomicLong kafkaSentBytes = new AtomicLong();
    private final Instant start = Instant.now();

    // MQTT → Kafka flow
    public void onMqttReceived(int byteLen) {
        mqttReceivedCount.incrementAndGet();
        mqttReceivedBytes.addAndGet(byteLen);
    }

    // RabbitMQ → Kafka flow
    public void onRabbitReceived(int byteLen) {
        rabbitReceivedCount.incrementAndGet();
        rabbitReceivedBytes.addAndGet(byteLen);
    }

    // 실제 Kafka 전송
    public void handleFromRabbit(byte[] body) {
        String payload = new String(body, StandardCharsets.UTF_8);
        kafkaTemplate.send(kafkaTopic, payload);
        kafkaSentCount.incrementAndGet();
        kafkaSentBytes.addAndGet(payload.getBytes(StandardCharsets.UTF_8).length);
    }

    // getters
    public long getMqttReceivedCount() {
        return mqttReceivedCount.get();
    }

    public long getMqttReceivedBytes() {
        return mqttReceivedBytes.get();
    }

    public double getMqttReceiveRatePerSec() {
        double s = (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1_000.0;
        return s > 0 ? mqttReceivedCount.get() / s : 0;
    }

    public long getRabbitReceivedCount() {
        return rabbitReceivedCount.get();
    }

    public long getRabbitReceivedBytes() {
        return rabbitReceivedBytes.get();
    }

    public double getRabbitReceiveRatePerSec() {
        double s = (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1_000.0;
        return s > 0 ? rabbitReceivedCount.get() / s : 0;
    }

    public long getKafkaSentCount() {
        return kafkaSentCount.get();
    }

    public long getKafkaSentBytes() {
        return kafkaSentBytes.get();
    }

    public double getKafkaSendRatePerSec() {
        double s = (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1_000.0;
        return s > 0 ? kafkaSentCount.get() / s : 0;
    }
}
