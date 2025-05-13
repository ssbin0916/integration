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

    // ========== 메트릭 변수 ==========
    private final AtomicLong mqttReceivedCount = new AtomicLong();
    private final AtomicLong mqttReceivedBytes = new AtomicLong();
    private final AtomicLong rabbitReceivedCount = new AtomicLong();
    private final AtomicLong rabbitReceivedBytes = new AtomicLong();
    private final AtomicLong kafkaSentCount = new AtomicLong();
    private final AtomicLong kafkaSentBytes = new AtomicLong();

    // 테스트 시작 시각
    private final Instant start = Instant.now();

    // ================================
    // 1) MQTT → Kafka 흐름
    // ================================

    /**
     * MQTT 메시지 수신 시 메트릭 집계만
     */
    public void onMqttReceived(int byteLen) {
        mqttReceivedCount.incrementAndGet();
        mqttReceivedBytes.addAndGet(byteLen);
    }

    /**
     * MQTT에서 수신한 문자열 페이로드를 Kafka로 전송
     */
    public void handleFromMqtt(String payload) {
        // Kafka 전송
        kafkaTemplate.send(kafkaTopic, payload);
        // 메트릭 집계
        kafkaSentCount.incrementAndGet();
        kafkaSentBytes.addAndGet(payload.getBytes(StandardCharsets.UTF_8).length);
    }

    // ================================
    // 2) RabbitMQ → Kafka 흐름
    // ================================

    /**
     * Rabbit 메시지 수신 시 메트릭 집계만
     */
    public void onRabbitReceived(int byteLen) {
        rabbitReceivedCount.incrementAndGet();
        rabbitReceivedBytes.addAndGet(byteLen);
    }

    /**
     * RabbitMQ에서 수신한 raw byte[] 페이로드를 Kafka로 전송
     */
    public void handleFromRabbit(byte[] body) {
        String payload = new String(body, StandardCharsets.UTF_8);
        // Kafka 전송
        kafkaTemplate.send(kafkaTopic, payload);
        // 메트릭 집계
        kafkaSentCount.incrementAndGet();
        kafkaSentBytes.addAndGet(payload.getBytes(StandardCharsets.UTF_8).length);
    }

    // ========== Getter: MQTT ==========
    public long getMqttReceivedCount() {
        return mqttReceivedCount.get();
    }

    public long getMqttReceivedBytes() {
        return mqttReceivedBytes.get();
    }

    public double getMqttReceiveRatePerSec() {
        double secs = (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1_000.0;
        return secs > 0 ? mqttReceivedCount.get() / secs : 0;
    }

    // ========== Getter: RabbitMQ =======
    public long getRabbitReceivedCount() {
        return rabbitReceivedCount.get();
    }

    public long getRabbitReceivedBytes() {
        return rabbitReceivedBytes.get();
    }

    public double getRabbitReceiveRatePerSec() {
        double secs = (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1_000.0;
        return secs > 0 ? rabbitReceivedCount.get() / secs : 0;
    }

    // ========== Getter: Kafka ==========
    public long getKafkaSentCount() {
        return kafkaSentCount.get();
    }

    public long getKafkaSentBytes() {
        return kafkaSentBytes.get();
    }

    public double getKafkaSendRatePerSec() {
        double secs = (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1_000.0;
        return secs > 0 ? kafkaSentCount.get() / secs : 0;
    }
}