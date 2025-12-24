package org.oldvabik.paymentservice.unit.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.*;
import org.oldvabik.paymentservice.client.RandomNumberClient;
import org.oldvabik.paymentservice.dto.PaymentCreateDto;
import org.oldvabik.paymentservice.dto.PaymentDto;
import org.oldvabik.paymentservice.entity.Payment;
import org.oldvabik.paymentservice.entity.PaymentStatus;
import org.oldvabik.paymentservice.kafka.PaymentEventProducer;
import org.oldvabik.paymentservice.mapper.PaymentMapper;
import org.oldvabik.paymentservice.repository.PaymentRepository;
import org.oldvabik.paymentservice.service.impl.PaymentServiceImpl;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PaymentServiceImplTest {

    @Mock
    private PaymentRepository paymentRepository;
    @Mock
    private PaymentMapper paymentMapper;
    @Mock
    private RandomNumberClient randomNumberClient;
    @Mock
    private PaymentEventProducer paymentEventProducer;

    @InjectMocks
    private PaymentServiceImpl paymentService;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void testSearch_ByOrderId() {
        Payment p1 = Payment.builder().orderId("order1").userId("user1").status(PaymentStatus.SUCCESS).build();
        Payment p2 = Payment.builder().orderId("order2").userId("user2").status(PaymentStatus.FAILED).build();
        when(paymentRepository.findAll()).thenReturn(List.of(p1, p2));
        when(paymentMapper.toDtoList(anyList())).thenAnswer(invocation -> {
            List<Payment> list = invocation.getArgument(0);
            return list.stream().map(p -> new PaymentDto()).toList();
        });

        Page<PaymentDto> page = paymentService.search(PageRequest.of(0, 10), "order1", null, null);

        assertEquals(1, page.getContent().size());
    }

    @Test
    void testSearch_ByUserId() {
        Payment p1 = Payment.builder().orderId("order1").userId("user1").status(PaymentStatus.SUCCESS).build();
        Payment p2 = Payment.builder().orderId("order2").userId("user2").status(PaymentStatus.FAILED).build();
        when(paymentRepository.findAll()).thenReturn(List.of(p1, p2));
        when(paymentMapper.toDtoList(anyList())).thenAnswer(invocation -> {
            List<Payment> list = invocation.getArgument(0);
            return list.stream().map(p -> new PaymentDto()).toList();
        });

        Page<PaymentDto> page = paymentService.search(PageRequest.of(0, 10), null, "user2", null);

        assertEquals(1, page.getContent().size());
    }

    @Test
    void testSearch_ByStatuses() {
        Payment p1 = Payment.builder().orderId("order1").userId("user1").status(PaymentStatus.SUCCESS).build();
        Payment p2 = Payment.builder().orderId("order2").userId("user2").status(PaymentStatus.FAILED).build();
        when(paymentRepository.findAll()).thenReturn(List.of(p1, p2));
        when(paymentMapper.toDtoList(anyList())).thenAnswer(invocation -> {
            List<Payment> list = invocation.getArgument(0);
            return list.stream().map(p -> new PaymentDto()).toList();
        });

        Page<PaymentDto> page = paymentService.search(PageRequest.of(0, 10), null, null, List.of(PaymentStatus.SUCCESS));

        assertEquals(1, page.getContent().size());
    }

    @Test
    void testSearch_NoFilter() {
        Payment p1 = Payment.builder().orderId("order1").userId("user1").status(PaymentStatus.SUCCESS).build();
        Payment p2 = Payment.builder().orderId("order2").userId("user2").status(PaymentStatus.FAILED).build();
        when(paymentRepository.findAll()).thenReturn(List.of(p1, p2));
        when(paymentMapper.toDtoList(anyList())).thenAnswer(invocation -> {
            List<Payment> list = invocation.getArgument(0);
            return list.stream().map(p -> new PaymentDto()).toList();
        });

        Page<PaymentDto> page = paymentService.search(PageRequest.of(0, 10), null, null, null);

        assertEquals(2, page.getContent().size());
    }

    @Test
    void testCreatePayment_FailedStatus() {
        PaymentCreateDto dto = new PaymentCreateDto();
        dto.setOrderId("order2");
        dto.setUserId("user2");
        dto.setPaymentAmount(BigDecimal.valueOf(200));

        when(randomNumberClient.getRandomNumber()).thenReturn(3); // odd -> FAILED

        Payment saved = Payment.builder()
                .id("2")
                .orderId(dto.getOrderId())
                .userId(dto.getUserId())
                .paymentAmount(dto.getPaymentAmount())
                .status(PaymentStatus.FAILED)
                .timestamp(LocalDateTime.now())
                .build();

        when(paymentRepository.save(any(Payment.class))).thenReturn(saved);
        when(paymentMapper.toDto(saved)).thenReturn(new PaymentDto());

        PaymentDto result = paymentService.createPayment(dto);

        assertNotNull(result);
        verify(paymentEventProducer, times(1)).sendPaymentCreatedEvent(any());
        verify(paymentRepository, times(1)).save(any(Payment.class));
    }

    @Test
    void testSearch_ByOrderIdAndUserId() {
        Payment p1 = Payment.builder().orderId("order1").userId("user1").status(PaymentStatus.SUCCESS).build();
        Payment p2 = Payment.builder().orderId("order1").userId("user2").status(PaymentStatus.FAILED).build();
        Payment p3 = Payment.builder().orderId("order2").userId("user1").status(PaymentStatus.SUCCESS).build();
        when(paymentRepository.findAll()).thenReturn(List.of(p1, p2, p3));
        when(paymentMapper.toDtoList(anyList())).thenAnswer(invocation -> {
            List<Payment> list = invocation.getArgument(0);
            return list.stream().map(p -> new PaymentDto()).toList();
        });

        Page<PaymentDto> page = paymentService.search(PageRequest.of(0, 10), "order1", "user1", null);

        assertEquals(1, page.getContent().size()); // Только p1 подходит под оба фильтра
    }

    @Test
    void testSearch_ByUserIdAndStatuses() {
        Payment p1 = Payment.builder().orderId("order1").userId("user1").status(PaymentStatus.SUCCESS).build();
        Payment p2 = Payment.builder().orderId("order2").userId("user1").status(PaymentStatus.FAILED).build();
        Payment p3 = Payment.builder().orderId("order3").userId("user2").status(PaymentStatus.SUCCESS).build();
        when(paymentRepository.findAll()).thenReturn(List.of(p1, p2, p3));
        when(paymentMapper.toDtoList(anyList())).thenAnswer(invocation -> {
            List<Payment> list = invocation.getArgument(0);
            return list.stream().map(p -> new PaymentDto()).toList();
        });

        Page<PaymentDto> page = paymentService.search(
                PageRequest.of(0, 10), null, "user1", List.of(PaymentStatus.SUCCESS));

        assertEquals(1, page.getContent().size()); // Только p1 подходит под оба фильтра
    }

    @Test
    void testCreatePayment_SuccessStatus() {
        PaymentCreateDto dto = new PaymentCreateDto();
        dto.setOrderId("order1");
        dto.setUserId("user1");
        dto.setPaymentAmount(BigDecimal.valueOf(100));

        when(randomNumberClient.getRandomNumber()).thenReturn(2); // even -> SUCCESS

        Payment saved = Payment.builder()
                .id("1")
                .orderId(dto.getOrderId())
                .userId(dto.getUserId())
                .paymentAmount(dto.getPaymentAmount())
                .status(PaymentStatus.SUCCESS)
                .timestamp(LocalDateTime.now())
                .build();

        when(paymentRepository.save(any(Payment.class))).thenReturn(saved);
        when(paymentMapper.toDto(saved)).thenReturn(new PaymentDto());

        PaymentDto result = paymentService.createPayment(dto);

        assertNotNull(result);
        verify(paymentEventProducer, times(1)).sendPaymentCreatedEvent(any());
        verify(paymentRepository, times(1)).save(any(Payment.class));
    }

    @Test
    void testGetTotalAmount_EmptyList() {
        when(paymentRepository.findSuccessfulPaymentsByDateRange(any(), any())).thenReturn(List.of());

        BigDecimal total = paymentService.getTotalAmount(LocalDateTime.now().minusDays(1), LocalDateTime.now());

        assertEquals(BigDecimal.ZERO, total);
    }
}
