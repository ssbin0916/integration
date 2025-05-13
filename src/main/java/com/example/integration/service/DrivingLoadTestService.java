package com.example.integration.service;

import com.example.integration.config.IntegrationConfig.MqttGateway;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class DrivingLoadTestService {

    private final MqttGateway mqttGateway;
    private final TaskScheduler taskScheduler;

    @Value("${load.test.messages-per-batch}")
    private int messagesPerBatch;

    // 주행 구간: 0.2초(200ms)에 1000개
    private static final long DRIVE_INTERVAL_MS = 200L;
    private static final int DRIVE_BATCH_SIZE = 1000;

    // 유휴 구간: 60초(60000ms)에 1개
    private static final long IDLE_INTERVAL_MS = 60_000L;
    private static final int IDLE_BATCH_SIZE = 1;

    private final AtomicLong sentCount = new AtomicLong();
    private final AtomicLong sentBytes = new AtomicLong();
    private final Instant startTime = Instant.now();

    private List<String> preGenerated;

    private static class ScheduleEntry {
        private final LocalTime from;
        private final LocalTime to;
        private final long intervalMs;
        private final int batchSize;

        ScheduleEntry(LocalTime from, LocalTime to, long intervalMs, int batchSize) {
            this.from = from;
            this.to = to;
            this.intervalMs = intervalMs;
            this.batchSize = batchSize;
        }

        boolean inRange(LocalTime now) {
            if (from.isBefore(to)) {
                return !now.isBefore(from) && now.isBefore(to);
            } else {
                // 자정 넘어가는 구간
                return !now.isBefore(from) || now.isBefore(to);
            }
        }
    }

    private final List<ScheduleEntry> schedule = List.of(
            new ScheduleEntry(LocalTime.of(6, 30), LocalTime.of(8, 30), DRIVE_INTERVAL_MS, DRIVE_BATCH_SIZE),
            new ScheduleEntry(LocalTime.of(8, 30), LocalTime.of(17, 0), IDLE_INTERVAL_MS, IDLE_BATCH_SIZE),
            new ScheduleEntry(LocalTime.of(17, 0), LocalTime.of(19, 0), DRIVE_INTERVAL_MS, DRIVE_BATCH_SIZE),
            new ScheduleEntry(LocalTime.of(19, 0), LocalTime.of(6, 30), IDLE_INTERVAL_MS, IDLE_BATCH_SIZE)
    );

    @PostConstruct
    public void init() {
        Random random = new Random();
        preGenerated = new ArrayList<>(messagesPerBatch);
        for (int i = 0; i < messagesPerBatch; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append("{\"id\":\"")
                    .append(UUID.randomUUID().toString())
                    .append("\"");
            for (int c = 1; c <= 199; c++) {
                double value = random.nextDouble();
                sb.append(",\"col")
                        .append(c)
                        .append("\":")
                        .append(value);
            }
            sb.append('}');
            preGenerated.add(sb.toString());
        }
        scheduleNext();
    }

    private void scheduleNext() {
        LocalTime now = LocalTime.now();
        ScheduleEntry currentEntry = schedule.get(0);
        for (ScheduleEntry entry : schedule) {
            if (entry.inRange(now)) {
                currentEntry = entry;
                break;
            }
        }

        // 즉시 배치 전송
        sendBatch(currentEntry.batchSize);

        // 다음 실행 예약
        Instant nextRun = Instant.now().plusMillis(currentEntry.intervalMs);
        taskScheduler.schedule(this::scheduleNext, nextRun);
    }

    private void sendBatch(int batchSize) {
        long bytesThisBatch = 0L;
        for (int i = 0; i < batchSize; i++) {
            String payload = preGenerated.get(i % preGenerated.size());
            mqttGateway.sendToMqtt(
                    org.springframework.integration.support.MessageBuilder
                            .withPayload(payload)
                            .setHeader("mqtt_retained", false)
                            .build()
            );
            sentCount.incrementAndGet();
            bytesThisBatch += payload.getBytes(StandardCharsets.UTF_8).length;
        }
        sentBytes.addAndGet(bytesThisBatch);
    }

    // === 조회용 메서드 ===

    public long getSentCount() {
        return sentCount.get();
    }

    public long getSentBytes() {
        return sentBytes.get();
    }

    public double getSendRatePerSec() {
        double seconds = (Instant.now().toEpochMilli() - startTime.toEpochMilli()) / 1000.0;
        return seconds > 0 ? sentCount.get() / seconds : 0;
    }
}
