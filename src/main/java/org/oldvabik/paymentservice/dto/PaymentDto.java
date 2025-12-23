package org.oldvabik.paymentservice.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.oldvabik.paymentservice.entity.PaymentStatus;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentDto {
    private String id;
    private String orderId;
    private String userId;
    private PaymentStatus status;
    private LocalDateTime timestamp;
    private BigDecimal paymentAmount;
}

