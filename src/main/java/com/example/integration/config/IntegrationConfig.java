package com.example.integration.config;

import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
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

@Configuration
public class IntegrationConfig {

    @Value("${spring.rabbitmq.queue}")
    private String rabbitQueue;

    @Value("${mqtt.broker-url}")
    private String mqttBrokerUrl;
    @Value("${mqtt.client-id}")
    private String mqttClientId;
    @Value("${mqtt.topic}")
    private String mqttTopic;
    @Value("${mqtt.qos}")
    private int mqttQos;

    @Value("${spring.kafka.topic}")
    private String kafkaTopic;

    // MQTT Publish support
    @Bean
    public MessageChannel mqttExecutorChannel() {
        return new DirectChannel();
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttExecutorChannel")
    public MessageHandler mqttPahoMessageHandler(MqttPahoClientFactory factory) {
        MqttPahoMessageHandler handler =
                new MqttPahoMessageHandler(mqttClientId + "-outbound", factory);
        handler.setAsync(true);
        handler.setConverter(new DefaultPahoMessageConverter());
        handler.setDefaultQos(mqttQos);
        return handler;
    }

    @MessagingGateway(defaultRequestChannel = "mqttExecutorChannel")
    public interface MqttGateway {
        void sendToMqtt(Message<String> msg);
    }

    // MQTT Subscribe → Kafka
    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[]{mqttBrokerUrl});
        options.setCleanSession(false);
        options.setAutomaticReconnect(true);
        options.setConnectionTimeout(30);
        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow mqttInboundFlow(
            MqttPahoClientFactory clientFactory,
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter(
                        mqttClientId + "-inbound", clientFactory, mqttTopic
                );
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(mqttQos);

        return IntegrationFlow.from(adapter)
                .channel(mqttInputChannel())
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate)
                        .topic(kafkaTopic))
                .get();
    }

    // RabbitMQ Subscribe → Kafka
    @Bean
    public MessageChannel rabbitInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow rabbitToKafkaFlow(
            ConnectionFactory connectionFactory,
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        SimpleMessageListenerContainer container =
                new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(rabbitQueue);
        container.setConcurrentConsumers(8);
        container.setPrefetchCount(1000);

        AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(container);
        adapter.setOutputChannel(rabbitInputChannel());

        return IntegrationFlow.from(adapter)
                .transform((byte[] p) -> new String(p, StandardCharsets.UTF_8))
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate)
                        .topic(kafkaTopic))
                .get();
    }
}
