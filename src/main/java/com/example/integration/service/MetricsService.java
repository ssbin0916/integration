package com.example.integration.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class MetricsService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${spring.kafka.topic}")
    private String kafkaTopic;

    // ========== 누적 메트릭 변수 ==========
    private final AtomicLong rabbitReceivedCount = new AtomicLong();
    private final AtomicLong rabbitReceivedBytes = new AtomicLong();
    private final AtomicLong kafkaReceiveCount = new AtomicLong();
    private final AtomicLong kafkaReceiveBytes = new AtomicLong();

    // ========== 실시간 스냅샷용 ==========
    private volatile long lastRabbitCount = 0;
    private volatile long lastKafkaCount = 0;
    private volatile Instant lastSnapshot = Instant.now();

    public void onRabbitReceived(int byteLength) {
        rabbitReceivedCount.incrementAndGet();
        rabbitReceivedBytes.addAndGet(byteLength);
    }

    public void handleFromRabbit(byte[] body) {
        String payload = new String(body, StandardCharsets.UTF_8);
        kafkaTemplate.send(kafkaTopic, payload);
    }

    public void onKafkaReceived(int byteLength) {
        kafkaReceiveCount.incrementAndGet();
        kafkaReceiveBytes.addAndGet(byteLength);
    }

    public synchronized Map<String, Object> getRealtimeMetrics() {
        Instant now = Instant.now();
        double secs = Duration.between(lastSnapshot, now).toMillis() / 1000.0;

        long currRabbit = rabbitReceivedCount.get();
        long currKafka = kafkaReceiveCount.get();

        double rabbitRate = secs > 0 ? (currRabbit - lastRabbitCount) / secs : 0;
        double kafkaRate = secs > 0 ? (currKafka - lastKafkaCount) / secs : 0;

        lastRabbitCount = currRabbit;
        lastKafkaCount = currKafka;
        lastSnapshot = now;

        LinkedHashMap<String, Object> m = new LinkedHashMap<>();

        m.put("rabbitCount", currRabbit);
        m.put("rabbitBytes", rabbitReceivedBytes.get());
        m.put("rabbitRatePerSec", rabbitRate);

        m.put("kafkaCount", currKafka);
        m.put("kafkaBytes", kafkaReceiveBytes.get());
        m.put("kafkaRatePerSec", kafkaRate);
        return m;
    }
}