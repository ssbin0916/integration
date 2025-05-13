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


    @Value("${mqtt.topic}")
    private String mqttTopic;

    @Value("${load.test.messages-per-batch:1000}")
    private int messagesPerBatch;

    @Value("${load.test.batch-interval:200}")
    private long batchInterval;

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
        for (String payload : preGenerated) {
            Message<String> msg = MessageBuilder
                    .withPayload(payload)
                    .setHeader(MqttHeaders.TOPIC, mqttTopic)
                    .build();
            mqttGateway.sendToMqtt(msg);
            sentCount.incrementAndGet();
            sentBytes.addAndGet(payload.getBytes(StandardCharsets.UTF_8).length);
        }
        System.out.println("[LoadTest] Sent batch of " + messagesPerBatch
                + " messages (total=" + sentCount.get() + ")");
    }

    public long getSentBytes() {
        return sentBytes.get(); // 추가
    }

    public int getMessagesPerBatch() {
        return messagesPerBatch; // 추가
    }

    public void reset() {
        sentCount.set(0);
    }

    public long getSentCount() {
        return sentCount.get();
    }
}
