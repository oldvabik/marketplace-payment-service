package org.oldvabik.paymentservice.service;

import org.oldvabik.paymentservice.dto.PaymentCreateDto;
import org.oldvabik.paymentservice.dto.PaymentDto;
import org.oldvabik.paymentservice.entity.PaymentStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public interface PaymentService {
    PaymentDto createPayment(PaymentCreateDto dto);

    Page<PaymentDto> search(Pageable pageable, String orderId, String userId, List<PaymentStatus> statuses);

    BigDecimal getTotalAmount(LocalDateTime from, LocalDateTime to);
}
