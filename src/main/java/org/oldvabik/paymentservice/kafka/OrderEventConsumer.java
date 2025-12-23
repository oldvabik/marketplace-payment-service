package org.oldvabik.paymentservice.kafka;

import lombok.extern.slf4j.Slf4j;
import org.oldvabik.paymentservice.dto.PaymentCreateDto;
import org.oldvabik.paymentservice.event.CreateOrderEvent;
import org.oldvabik.paymentservice.service.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class OrderEventConsumer {
    private final PaymentService paymentService;

    public OrderEventConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = "${app.kafka.create-order-topic:order-created-topic}", 
                   groupId = "${app.kafka.payment-group-id:payment-service-group}")
    public void consumeCreateOrderEvent(CreateOrderEvent event) {
        log.info("[OrderEventConsumer] Received CREATE_ORDER event: orderId={}, userId={}, amount={}", 
            event.getOrderId(), event.getUserId(), event.getTotalAmount());

        try {
            PaymentCreateDto paymentDto = PaymentCreateDto.builder()
                    .orderId(event.getOrderId())
                    .userId(event.getUserId())
                    .paymentAmount(event.getTotalAmount())
                    .build();

            paymentService.createPayment(paymentDto);
            log.info("[OrderEventConsumer] Payment created successfully for orderId={}", event.getOrderId());
        } catch (Exception e) {
            log.error("[OrderEventConsumer] Error processing CREATE_ORDER event for orderId={}: {}", 
                event.getOrderId(), e.getMessage(), e);
        }
    }
}

