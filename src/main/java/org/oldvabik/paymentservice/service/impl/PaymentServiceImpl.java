package org.oldvabik.paymentservice.service.impl;

import org.oldvabik.paymentservice.client.RandomNumberClient;
import org.oldvabik.paymentservice.dto.PaymentCreateDto;
import org.oldvabik.paymentservice.dto.PaymentDto;
import org.oldvabik.paymentservice.entity.Payment;
import org.oldvabik.paymentservice.entity.PaymentStatus;
import org.oldvabik.paymentservice.event.CreatePaymentEvent;
import org.oldvabik.paymentservice.kafka.PaymentEventProducer;
import org.oldvabik.paymentservice.mapper.PaymentMapper;
import org.oldvabik.paymentservice.repository.PaymentRepository;
import org.oldvabik.paymentservice.service.PaymentService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class PaymentServiceImpl implements PaymentService {
    private final PaymentRepository paymentRepository;
    private final PaymentMapper paymentMapper;
    private final RandomNumberClient randomNumberClient;
    private final PaymentEventProducer paymentEventProducer;

    public PaymentServiceImpl(PaymentRepository paymentRepository, PaymentMapper paymentMapper,
                              RandomNumberClient randomNumberClient, PaymentEventProducer paymentEventProducer) {
        this.paymentRepository = paymentRepository;
        this.paymentMapper = paymentMapper;
        this.randomNumberClient = randomNumberClient;
        this.paymentEventProducer = paymentEventProducer;
    }

    @Override
    @Transactional
    public PaymentDto createPayment(PaymentCreateDto dto) {

        int randomNumber = randomNumberClient.getRandomNumber();
        PaymentStatus status = randomNumber % 2 == 0 ? PaymentStatus.SUCCESS : PaymentStatus.FAILED;

        Payment payment = Payment.builder()
                .orderId(dto.getOrderId())
                .userId(dto.getUserId())
                .paymentAmount(dto.getPaymentAmount())
                .status(status)
                .timestamp(LocalDateTime.now())
                .build();

        Payment saved = paymentRepository.save(payment);

        paymentEventProducer.sendPaymentCreatedEvent(
                CreatePaymentEvent.builder()
                        .paymentId(saved.getId())
                        .orderId(saved.getOrderId())
                        .userId(saved.getUserId())
                        .status(saved.getStatus().name())
                        .paymentAmount(saved.getPaymentAmount())
                        .timestamp(saved.getTimestamp())
                        .build()
        );

        return paymentMapper.toDto(saved);
    }

    @Override
    public Page<PaymentDto> search(Pageable pageable, String orderId, String userId, List<PaymentStatus> statuses) {
        List<Payment> filtered = paymentRepository.findAll().stream()
                .filter(p -> (orderId == null || p.getOrderId().equals(orderId)))
                .filter(p -> (userId == null || p.getUserId().equals(userId)))
                .filter(p -> (statuses == null || statuses.isEmpty() || statuses.contains(p.getStatus())))
                .collect(Collectors.toList());

        int start = (int) pageable.getOffset();
        int end = Math.min(start + pageable.getPageSize(), filtered.size());

        return new PageImpl<>(paymentMapper.toDtoList(filtered.subList(start, end)), pageable, filtered.size());
    }


    @Override
    public BigDecimal getTotalAmount(LocalDateTime from, LocalDateTime to) {
        return paymentRepository.findSuccessfulPaymentsByDateRange(from, to).stream()
                .map(Payment::getPaymentAmount)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }
}
