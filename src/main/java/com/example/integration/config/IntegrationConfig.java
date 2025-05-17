package com.example.integration.config;

import com.example.integration.service.MetricsService;
import com.rabbitmq.client.Channel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.inbound.KafkaMessageDrivenChannelAdapter;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class IntegrationConfig {

    // === RabbitMQ 설정 값들 주입 ===
    @Value("${spring.rabbitmq.queue}")
    private String queue;

    // === Kafka 설정 값들 주입 ===
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    @Value("${spring.kafka.topic}")
    private String topic;


    // ------------------------------------------------------------
    // 1) RabbitMQ → Kafka 통합 Flow
    //    - SimpleMessageListenerContainer 로 RabbitMQ에서 메시지를 수동 ACK 모드로 소비
    //    - Spring Integration 의 AmqpInboundChannelAdapter 를 통해 IntegrationFlow 시작
    //    - 메트릭 집계 및 Kafka 전송 후 수동으로 basicAck 수행
    // ------------------------------------------------------------
    @Bean
    public IntegrationFlow rabbitToKafkaFlow(
            ConnectionFactory connectionFactory,
            MetricsService metricsService
    ) {
        // 1.1) 리스너 컨테이너 설정: 경쟁 소비자 8개, prefetch 625, MANUAL ACK
        SimpleMessageListenerContainer container =
                new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(queue);
        container.setConcurrentConsumers(8);
        container.setPrefetchCount(625);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);

        // 1.2) AMQP Inbound Adapter 생성
        AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(container);

        // 1.3) Integration Flow 정의
        return IntegrationFlow.from(adapter)
                .handle(Message.class, (payloadMessage, headers) -> {
                    // 1.3.1) 바디를 byte[] 로 가져오기
                    byte[] body = (byte[]) payloadMessage.getPayload();

                    // 1.3.2) RabbitMQ 수신 메트릭 집계
                    metricsService.onRabbitReceived(body.length);

                    // 1.3.3) Kafka 전송
                    metricsService.handleFromRabbit(body);

                    // 1.3.4) 수동 ACK: RabbitMQ 채널과 deliveryTag 헤더에서 꺼내 basicAck 호출
                    Channel channel = (Channel) headers.get(AmqpHeaders.CHANNEL);
                    long deliveryTag = (long) headers.get(AmqpHeaders.DELIVERY_TAG);
                    try {
                        channel.basicAck(deliveryTag, false);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to ack RabbitMQ message", e);
                    }

                    // 1.3.5) Flow 종료: 반환값 없음(null)
                    return null;
                })
                .get();
    }

    // ------------------------------------------------------------
    // 2) Kafka ConsumerFactory 빈 정의
    //    - Manual commit, batch size, fetch size 등 최적화
    // ------------------------------------------------------------
    @Bean
    public ConsumerFactory<String, String> kafkaConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        // 클러스터 주소
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // Consumer 그룹
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // 직렬화 클래스 지정
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 최대 polling 레코드 수
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        // 최소/최대 fetch 바이트
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 16 * 1024);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 1024 * 1024);
        // 자동 커밋 끄기 → 수동 즉시 커밋 모드
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // 오프셋 리셋 정책
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ------------------------------------------------------------
    // 3) Kafka Listener Container 설정
    //    - 병렬 컨슈머 8개, 수동 즉시 ACK 모드
    // ------------------------------------------------------------
    @Bean
    public ConcurrentMessageListenerContainer<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory
    ) {
        // Kafka 토픽 구독 설정
        ContainerProperties containerProps = new ContainerProperties(topic);
        // 레코드 단위로 ACK
        containerProps.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProps);
        // 병렬 처리 쓰레드 수
        container.setConcurrency(8);
        return container;
    }

    // ------------------------------------------------------------
    // 4) Kafka → 메트릭 집계 Flow
    //    - 메시지 수신 시 메트릭 카운터 증가 및 수동 ACK
    // ------------------------------------------------------------
    @Bean
    public IntegrationFlow kafkaInboundFlow(
            ConcurrentMessageListenerContainer<String, String> container,
            MetricsService metricsService
    ) {
        return IntegrationFlow
                // 4.1) Spring Integration kafka 어댑터: batch 모드
                .from(Kafka.messageDrivenChannelAdapter(
                        container,
                        KafkaMessageDrivenChannelAdapter.ListenerMode.batch
                ))
                // 4.2) 배치 단위로 수신된 List<Message> 를 개별 메시지로 split
                .split()
                .handle(Message.class, (message, headers) -> {
                    // 4.3) 페이로드를 String 으로 변환
                    String payload = (String) message.getPayload();
                    int length = payload.getBytes(StandardCharsets.UTF_8).length;

                    // 4.4) Kafka 수신 메트릭 집계
                    metricsService.onKafkaReceived(length);

                    // 4.5) 수동 ACK 호출
                    Acknowledgment ack = headers.get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);
                    if (ack != null) {
                        ack.acknowledge();
                    }

                    return null;
                })
                .get();
    }

}