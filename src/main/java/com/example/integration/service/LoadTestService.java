package com.example.integration.service;

import com.example.integration.config.IntegrationConfig.MqttGateway;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class LoadTestService {

    private final MqttGateway mqttGateway;
    private final Random random = new Random();
    private final AtomicLong sentCount = new AtomicLong();
    private final AtomicLong sentBytes = new AtomicLong();
    private final Instant start = Instant.now();
    private Instant startTime;


    @Value("${mqtt.topic}")
    private String mqttTopic;

    @Value("${load.test.messages-per-batch}")
    private int messagesPerBatch;

    @Value("${load.test.batch-interval}")
    private long batchInterval;

    @Value("${load.test.duration-seconds}")
    private long durationSeconds;

    private List<String> preGenerated;

    @PostConstruct
    public void init() {
        preGenerated = new ArrayList<>(messagesPerBatch);
        for (int i = 0; i < messagesPerBatch; i++) {
            String id = UUID.randomUUID().toString();
            StringBuilder sb = new StringBuilder();
            sb.append("{\"id\":\"")
                    .append(id)
                    .append("\"");
            for (int c = 1; c <= 199; c++) {
                double v = random.nextDouble();
                sb.append(",\"col")
                        .append(c)
                        .append("\":")
                        .append(v);
            }
            sb.append('}');
            preGenerated.add(sb.toString());
        }
    }

    @Scheduled(fixedRateString = "${load.test.batch-interval:200}")
    public void sendBatch() {
        if (startTime == null) {
            startTime = Instant.now();
            System.out.println("[LoadTest] Starting load test: duration=" + durationSeconds + "s, batchSize=" + messagesPerBatch);
        }

        long elapsed = Duration.between(startTime, Instant.now()).getSeconds();
        if (elapsed >= durationSeconds) {
            System.out.println("[LoadTest] Finished load test: elapsed=" + elapsed + "s, totalSent=" + sentCount.get());
            return;
        }
        long batchStart = System.currentTimeMillis();
        for (String payload : preGenerated) {
            Message<String> msg = MessageBuilder
                    .withPayload(payload)
                    .setHeader(MqttHeaders.TOPIC, mqttTopic)
                    .build();
            mqttGateway.sendToMqtt(msg);
            sentCount.incrementAndGet();
            sentBytes.addAndGet(payload.getBytes(StandardCharsets.UTF_8).length);
        }
        long schedulingTime = System.currentTimeMillis() - batchStart;
        System.out.println("[LoadTest] Sent batch of " + messagesPerBatch
                + " messages, schedulingTime=" + schedulingTime + "ms, totalSent=" + sentCount.get());
    }

    public long getSentCount() {
        return sentCount.get();
    }

    public long getSentBytes() {
        return sentBytes.get();
    }

    public double getSendRatePerSec() {
        double secs = (Instant.now().toEpochMilli() - start.toEpochMilli()) / 1_000.0;
        return secs > 0 ? sentCount.get() / secs : 0;
    }
}
