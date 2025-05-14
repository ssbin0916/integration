package com.example.integration.service;

import com.example.integration.config.IntegrationConfig.MqttGateway;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.integration.mqtt.support.MqttHeaders;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalTime;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class DrivingLoadTestService2 {

    private final MqttGateway mqttGateway;
    private final TaskScheduler taskScheduler;

    // 주행 구간: 0.2초(200ms)에 1000개
    private static final long DRIVE_INTERVAL_MS = 200L;
    private static final int DRIVE_BATCH_SIZE = 1000;

    // 유휴 구간: 60초(60000ms)에 1개
    private static final long IDLE_INTERVAL_MS = 60_000L;
    private static final int IDLE_BATCH_SIZE = 1;

    private final AtomicLong sentCount = new AtomicLong();
    private final AtomicLong sentBytes = new AtomicLong();
    private final Instant startTime = Instant.now();

    private final Random random = new Random();

    private record ScheduleEntry(LocalTime from, LocalTime to, long intervalMs, int batchSize) {
        boolean inRange(LocalTime now) {
                if (from.isBefore(to)) {
                    return !now.isBefore(from) && now.isBefore(to);
                }
                return !now.isBefore(from) || now.isBefore(to);
            }
        }

    private final ScheduleEntry[] schedule = new ScheduleEntry[] {
            new ScheduleEntry(LocalTime.of(6,30), LocalTime.of(8,30), DRIVE_INTERVAL_MS, DRIVE_BATCH_SIZE),
            new ScheduleEntry(LocalTime.of(8,30), LocalTime.of(17,0), IDLE_INTERVAL_MS, IDLE_BATCH_SIZE),
            new ScheduleEntry(LocalTime.of(17,0), LocalTime.of(19,0), DRIVE_INTERVAL_MS, DRIVE_BATCH_SIZE),
            new ScheduleEntry(LocalTime.of(19,0), LocalTime.of(6,30), IDLE_INTERVAL_MS, IDLE_BATCH_SIZE)
    };

    @PostConstruct
    public void init() {
        // 첫 스케줄 예약
        scheduleNext();
    }

    private void scheduleNext() {
        // 현재 시간대에 맞는 스케줄 항목을 찾는다
        LocalTime now = LocalTime.now();
        ScheduleEntry entry = schedule[0];
        for (ScheduleEntry e : schedule) {
            if (e.inRange(now)) { entry = e; break; }
        }

        // 즉시 배치 전송
        sendBatch(entry.batchSize);

        // 다음 실행을 예약
        Instant next = Instant.now().plusMillis(entry.intervalMs);
        taskScheduler.schedule(this::scheduleNext, next);
    }

    private void sendBatch(int batchSize) {
        long bytesThisBatch = 0;
        for (int i = 0; i < batchSize; i++) {
            String payload = generatePayload();
            mqttGateway.sendToMqtt(
                    org.springframework.integration.support.MessageBuilder
                            .withPayload(payload)
                            .setHeader(MqttHeaders.TOPIC, "test")
                            .build()
            );
            sentCount.incrementAndGet();
            bytesThisBatch += payload.getBytes(StandardCharsets.UTF_8).length;
        }
        sentBytes.addAndGet(bytesThisBatch);
    }

    /**
     * 매번 호출 시 새로운 JSON 문자열을 생성.
     * StringBuilder를 쓰되, 메서드 로컬 변수로 두어 GC 부담을 최소화합니다.
     */
    private String generatePayload() {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("{\"id\":\"").append(UUID.randomUUID()).append("\"");
        for (int c = 1; c <= 199; c++) {
            sb.append(",\"col").append(c).append("\":")
                    .append(random.nextDouble());
        }
        sb.append('}');
        return sb.toString();
    }

    // === 모니터링용 ===
    public long getSentCount()    { return sentCount.get();      }
    public long getSentBytes()    { return sentBytes.get();      }
    public double getSendRatePerSec() {
        double secs = (Instant.now().toEpochMilli() - startTime.toEpochMilli()) / 1_000.0;
        return secs > 0 ? sentCount.get() / secs : 0;
    }
}
