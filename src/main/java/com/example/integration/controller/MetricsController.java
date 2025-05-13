package com.example.integration.controller;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("/api/metrics")
@RequiredArgsConstructor
public class MetricsController {

    private final RabbitAdmin rabbitAdmin;
    private final KafkaAdmin kafkaAdmin;

    @Value("${spring.rabbitmq.queue}")
    private String rabbitQueue;

    @Value("${spring.kafka.topic}")
    private String kafkaTopic;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    /**
     * RabbitMQ 큐 메시지 개수 및 소비자 수
     */
    @GetMapping("/rabbitmq")
    public ResponseEntity<Map<String, Object>> rabbitMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        try {
            Properties props = rabbitAdmin.getQueueProperties(rabbitQueue);
            metrics.put("messageCount", props.get(RabbitAdmin.QUEUE_MESSAGE_COUNT));
            metrics.put("consumerCount", props.get(RabbitAdmin.QUEUE_CONSUMER_COUNT));
            return ResponseEntity.ok(metrics);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Kafka 토픽 end offsets 및 consumer group lag
     */
    @GetMapping("/kafka")
    public ResponseEntity<Map<String, Object>> kafkaMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        Properties configs = new Properties();
        configs.putAll(kafkaAdmin.getConfigurationProperties());
        try (AdminClient client = AdminClient.create(configs)) {
            // 토픽 존재 여부
            ListTopicsResult topicsResult = client.listTopics();
            if (!topicsResult.names().get().contains(kafkaTopic)) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "Topic not found"));
            }

            // 파티션별 end offset 조회
            TopicDescription desc = client.describeTopics(
                            java.util.List.of(kafkaTopic))
                    .all().get().get(kafkaTopic);
            long totalEndOffset = 0;
            for (var pi : desc.partitions()) {
                TopicPartition tp = new TopicPartition(kafkaTopic, pi.partition());
                ListOffsetsResult lr = client.listOffsets(
                        Map.of(tp, OffsetSpec.latest())
                );
                long offset = lr.partitionResult(tp).get().offset();
                totalEndOffset += offset;
            }
            metrics.put("endOffset", totalEndOffset);

            // Consumer group offsets 조회
            ListConsumerGroupOffsetsResult co = client.listConsumerGroupOffsets(consumerGroupId);
            var offsets = co.partitionsToOffsetAndMetadata().get();
            long totalConsumerOffset = offsets.entrySet().stream()
                    .filter(e -> e.getKey().topic().equals(kafkaTopic))
                    .mapToLong(e -> e.getValue().offset())
                    .sum();
            metrics.put("consumerOffset", totalConsumerOffset);

            metrics.put("lag", totalEndOffset - totalConsumerOffset);
            return ResponseEntity.ok(metrics);
        } catch (ExecutionException | InterruptedException e) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", e.getMessage()));
        }
    }
}