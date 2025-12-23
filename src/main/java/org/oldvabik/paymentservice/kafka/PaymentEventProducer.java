package org.oldvabik.paymentservice.kafka;

import lombok.extern.slf4j.Slf4j;
import org.oldvabik.paymentservice.event.CreatePaymentEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PaymentEventProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.create-payment-topic:payment-created-topic}")
    private String paymentTopic;

    public PaymentEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendPaymentCreatedEvent(CreatePaymentEvent event) {
        log.info("[PaymentEventProducer] Sending CREATE_PAYMENT event: paymentId={}, orderId={}", 
            event.getPaymentId(), event.getOrderId());

        kafkaTemplate.send(paymentTopic, event.getOrderId(), event);

        log.debug("[PaymentEventProducer] CREATE_PAYMENT event sent successfully");
    }
}

