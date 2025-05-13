package com.example.integration.config;

import com.example.integration.service.MqttToKafkaService;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.nio.charset.StandardCharsets;

/**
 * Spring Integration 설정 클래스
 * - MQTT 발행 및 구독
 * - RabbitMQ 구독
 * - Kafka 전송
 */
@Configuration
public class IntegrationConfig {

    // RabbitMQ 큐 이름 (application.yml에서 주입)
    @Value("${spring.rabbitmq.queue}")
    private String rabbitQueue;

    // MQTT 브로커 접속 URL (tcp://host:port)
    @Value("${mqtt.broker-url}")
    private String mqttBrokerUrl;

    // MQTT 클라이언트 식별자
    @Value("${mqtt.client-id}")
    private String mqttClientId;

    // MQTT 구독/발행 토픽
    @Value("${mqtt.topic}")
    private String mqttTopic;

    // MQTT QoS 레벨 (0, 1, 2)
    @Value("${mqtt.qos}")
    private int mqttQos;

    // Kafka 토픽 이름
    @Value("${spring.kafka.topic}")
    private String kafkaTopic;

    // ==========================
    // 1. MQTT 발행(Publish) 지원
    // ==========================

    /**
     * MqttGateway가 메시지를 전송할 채널
     */
    @Bean
    public MessageChannel mqttExecutorChannel() {
        return new DirectChannel();
    }

    /**
     * mqttExecutorChannel로 전달된 메시지를 실제 MQTT 브로커에 발행
     */
    @Bean
    @ServiceActivator(inputChannel = "mqttExecutorChannel")
    public MessageHandler mqttPahoMessageHandler(MqttPahoClientFactory factory) {
        MqttPahoMessageHandler handler =
                new MqttPahoMessageHandler(mqttClientId + "-outbound", factory);
        handler.setAsync(true);  // 비동기 전송
        handler.setConverter(new DefaultPahoMessageConverter());  // 페이로드 변환
        handler.setDefaultQos(mqttQos);  // QoS 설정
        return handler;
    }

    /**
     * 코드에서 mqttExecutorChannel로 메시지를 보낼 수 있도록 해주는 Gateway
     */
    @MessagingGateway(defaultRequestChannel = "mqttExecutorChannel")
    public interface MqttGateway {
        void sendToMqtt(Message<String> msg);
    }

    // ==================================
    // 2. MQTT 구독(Subscribe) 후 Kafka 전송
    // ==================================

    /**
     * Paho MQTT 클라이언트 팩토리 설정
     */
    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions opts = new MqttConnectOptions();
        opts.setServerURIs(new String[]{mqttBrokerUrl});  // 브로커 주소
        opts.setCleanSession(false);  // 세션 유지
        opts.setAutomaticReconnect(true);  // 자동 재연결
        opts.setConnectionTimeout(30);  // 연결 타임아웃
        factory.setConnectionOptions(opts);
        return factory;
    }

    /**
     * MQTT 수신 후 메시지를 전달할 채널
     */
    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    /**
     * MQTT 토픽을 구독하고, 수신된 메시지를 Kafka로 전송하는 Flow
     */
    @Bean
    public IntegrationFlow mqttInboundFlow(
            MqttPahoClientFactory clientFactory,
            MqttToKafkaService mqttToKafkaService
    ) {
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(
                        mqttClientId + "-inbound", clientFactory, mqttTopic
                );
        adapter.setConverter(new DefaultPahoMessageConverter());  // 페이로드 변환
        adapter.setQos(mqttQos);  // QoS 설정

        return IntegrationFlow.from(adapter)
                .handle(byte[].class, (payload, headers) -> {
                    // 1) 메트릭 집계
                    mqttToKafkaService.onMqttReceived(payload.length);
                    // 2) String 변환 후 Kafka 전송(내부에서 메트릭도 집계)
                    mqttToKafkaService.handleFromRabbit(payload);
                    return null;
                })
                .get();
    }

    // =====================================
    // 3. RabbitMQ 구독(Subscribe) 후 Kafka 전송
    // =====================================

    /**
     * RabbitMQ 수신 후 메시지를 전달할 채널
     */
    @Bean
    public MessageChannel rabbitInputChannel() {
        return new DirectChannel();
    }

    /**
     * RabbitMQ 큐를 소비하고, 메시지를 Kafka로 전송하는 Flow
     * - byte[] 페이로드를 String으로 변환 후 전송
     */
    @Bean
    public IntegrationFlow rabbitToKafkaFlow(
            ConnectionFactory connectionFactory,
            KafkaTemplate<String, String> kafkaTemplate,
            MqttToKafkaService mqttToKafkaService
    ) {
        SimpleMessageListenerContainer container =
                new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(rabbitQueue);  // 큐 이름
        container.setConcurrentConsumers(8);  // 동시 소비 스레드 수
        container.setPrefetchCount(1000);  // prefetch 수
        container.setAcknowledgeMode(AcknowledgeMode.AUTO);

        AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(container);
        adapter.setOutputChannel(rabbitInputChannel());  // 채널 연결

        return IntegrationFlow.from(adapter)
                .handle(byte[].class, (body, headers) -> {
                    mqttToKafkaService.onRabbitReceived(body.length);
                    mqttToKafkaService.handleFromRabbit(body);
                    return null;
                })
                .get();
    }
}
