package org.oldvabik.paymentservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreatePaymentEvent {
    private String paymentId;
    private String orderId;
    private String userId;
    private String status;
    private BigDecimal paymentAmount;
    private LocalDateTime timestamp;
}

