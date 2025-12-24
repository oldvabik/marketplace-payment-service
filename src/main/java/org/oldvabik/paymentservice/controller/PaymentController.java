package org.oldvabik.paymentservice.controller;

import org.oldvabik.paymentservice.dto.PaymentDto;
import org.oldvabik.paymentservice.entity.PaymentStatus;
import org.oldvabik.paymentservice.service.PaymentService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/v1/payments")
public class PaymentController {
    private final PaymentService paymentService;

    public PaymentController(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @PreAuthorize("hasRole('ADMIN')")
    @GetMapping("/search")
    public ResponseEntity<Page<PaymentDto>> searchPayments(Pageable pageable,
                                                           @RequestParam(required = false) String orderId,
                                                           @RequestParam(required = false) String userId,
                                                           @RequestParam(required = false) List<PaymentStatus> statuses) {
        Page<PaymentDto> payments = paymentService.search(pageable, orderId, userId, statuses);
        return new ResponseEntity<>(payments, HttpStatus.OK);
    }

    @PreAuthorize("hasRole('ADMIN')")
    @GetMapping("/summary")
    public ResponseEntity<BigDecimal> getTotalAmount(@RequestParam LocalDateTime from,
                                                     @RequestParam LocalDateTime to) {
        BigDecimal total = paymentService.getTotalAmount(from, to);
        return new ResponseEntity<>(total, HttpStatus.OK);
    }
}
