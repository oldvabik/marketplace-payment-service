package org.oldvabik.paymentservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest(properties = {
        "spring.data.mongodb.uri=mongodb://localhost:27017/testdb",
        "spring.kafka.bootstrap-servers=localhost:9092",
})
class PaymentServiceApplicationTests {

    @Test
    void contextLoads() {
    }

}
