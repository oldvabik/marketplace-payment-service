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
public class CreateOrderEvent {
    private String orderId;
    private String userId;
    private BigDecimal totalAmount;
    private LocalDateTime createdAt;
}

