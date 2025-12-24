package org.oldvabik.paymentservice.repository;

import org.oldvabik.paymentservice.entity.Payment;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface PaymentRepository extends MongoRepository<Payment, String> {
    @Query("{ 'timestamp': { $gte: ?0, $lte: ?1 }, 'status': 'SUCCESS' }")
    List<Payment> findSuccessfulPaymentsByDateRange(LocalDateTime from, LocalDateTime to);
}
