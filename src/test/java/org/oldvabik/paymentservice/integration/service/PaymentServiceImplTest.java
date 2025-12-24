package org.oldvabik.paymentservice.integration.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.oldvabik.paymentservice.dto.PaymentCreateDto;
import org.oldvabik.paymentservice.dto.PaymentDto;
import org.oldvabik.paymentservice.entity.PaymentStatus;
import org.oldvabik.paymentservice.event.CreatePaymentEvent;
import org.oldvabik.paymentservice.repository.PaymentRepository;
import org.oldvabik.paymentservice.service.impl.PaymentServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@Testcontainers
@TestPropertySource(properties = {
        "app.kafka.create-payment-topic=test-payment-created-topic"
})
class PaymentServiceImplTest {

    @Container
    static MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:6.0"))
            .withExposedPorts(27017);

    @Container
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"))
            .withEmbeddedZookeeper();

    @Autowired
    private PaymentServiceImpl paymentService;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private ObjectMapper objectMapper;

    private Consumer<String, CreatePaymentEvent> kafkaConsumer;

    private static com.github.tomakehurst.wiremock.WireMockServer wireMockServer;

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        wireMockServer = new com.github.tomakehurst.wiremock.WireMockServer(
                com.github.tomakehurst.wiremock.core.WireMockConfiguration.options().dynamicPort()
        );
        wireMockServer.start();

        registry.add("random.api.url", () -> "http://localhost:" + wireMockServer.port() + "/integers?num=1&min=1&max=100&col=1&base=10&format=plain");

        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("spring.data.mongodb.uri", mongoDBContainer::getReplicaSetUrl);
    }

    @BeforeAll
    static void beforeAll() {
        if (wireMockServer != null && !wireMockServer.isRunning()) {
            wireMockServer.start();
        }
    }

    @AfterAll
    static void afterAll() {
        if (wireMockServer != null) {
            wireMockServer.stop();
        }
    }

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAll();
        wireMockServer.resetAll();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-payment-consumer-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.TYPE_MAPPINGS, "CreatePaymentEvent:org.oldvabik.paymentservice.event.CreatePaymentEvent");

        kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Collections.singletonList("test-payment-created-topic"));
    }

    @AfterEach
    void tearDown() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }

    private List<ConsumerRecord<String, CreatePaymentEvent>> pollKafkaRecords(Duration timeout) {
        ConsumerRecords<String, CreatePaymentEvent> records = kafkaConsumer.poll(timeout);
        return StreamSupport.stream(records.spliterator(), false)
                .collect(Collectors.toList());
    }

    @Test
    void createPayment_ShouldCreateSuccessfulPaymentAndSendKafkaEvent() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/plain")
                        .withBody("42"))); // чётное → SUCCESS

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-123")
                .userId("user-456")
                .paymentAmount(new BigDecimal("99.99"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result).isNotNull();
        assertThat(result.getOrderId()).isEqualTo("order-123");
        assertThat(result.getUserId()).isEqualTo("user-456");
        assertThat(result.getPaymentAmount()).isEqualTo(new BigDecimal("99.99"));
        assertThat(result.getStatus()).isEqualTo(PaymentStatus.SUCCESS);

        await().atMost(15, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    List<ConsumerRecord<String, CreatePaymentEvent>> records = pollKafkaRecords(Duration.ofMillis(300));
                    assertThat(records).hasSizeGreaterThanOrEqualTo(1);
                    assertThat(records.get(0).key()).isEqualTo("order-123");
                    assertThat(records.get(0).value().getOrderId()).isEqualTo("order-123");
                    assertThat(records.get(0).value().getPaymentId()).isEqualTo(result.getId());
                    assertThat(records.get(0).value().getStatus()).isEqualTo("SUCCESS");
                    assertThat(records.get(0).value().getPaymentAmount()).isEqualTo(new BigDecimal("99.99"));
                });
    }

    @Test
    void createPayment_ShouldCreateFailedPaymentWhenRandomNumberIsOdd() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("1"))); // нечётное → FAILED

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-failed")
                .userId("user-failed")
                .paymentAmount(new BigDecimal("50.00"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result.getStatus()).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void createPayment_ShouldUseFallbackWhenApiReturnsError() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withStatus(500)));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-fallback")
                .userId("user-fallback")
                .paymentAmount(new BigDecimal("50.00"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result.getOrderId()).isEqualTo("order-fallback");
        assertThat(result.getStatus()).isIn(PaymentStatus.SUCCESS, PaymentStatus.FAILED);
    }

    @Test
    void createPayment_ShouldUseFallbackWhenApiTimesOut() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withFixedDelay(3000).withBody("10")));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-timeout")
                .userId("user-timeout")
                .paymentAmount(new BigDecimal("25.00"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result.getStatus()).isIn(PaymentStatus.SUCCESS, PaymentStatus.FAILED);
    }

    @Test
    void createPayment_ShouldUseFallbackForInvalidApiResponse() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("not-a-number")));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-invalid")
                .userId("user-invalid")
                .paymentAmount(new BigDecimal("75.00"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result.getStatus()).isIn(PaymentStatus.SUCCESS, PaymentStatus.FAILED);
    }

    @Test
    void createPayment_ShouldUseFallbackForEmptyApiResponse() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("")));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-empty")
                .userId("user-empty")
                .paymentAmount(new BigDecimal("88.88"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result.getStatus()).isIn(PaymentStatus.SUCCESS, PaymentStatus.FAILED);
    }

    @Test
    void createPayment_ShouldSendKafkaEventAsync() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("2")));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-kafka")
                .userId("user-kafka")
                .paymentAmount(new BigDecimal("123.45"))
                .build();

        PaymentDto payment = paymentService.createPayment(paymentCreateDto);

        await().atMost(15, TimeUnit.SECONDS)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    List<ConsumerRecord<String, CreatePaymentEvent>> records = pollKafkaRecords(Duration.ofMillis(300));
                    assertThat(records)
                            .filteredOn(r -> r.value().getPaymentId().equals(payment.getId()))
                            .isNotEmpty();
                });
    }

    @Test
    void createPayment_MultiplePayments_ShouldGenerateDifferentIds() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("2")));

        PaymentDto result1 = paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-1")
                .userId("user-1")
                .paymentAmount(new BigDecimal("10.00"))
                .build());

        PaymentDto result2 = paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-2")
                .userId("user-1")
                .paymentAmount(new BigDecimal("20.00"))
                .build());

        assertThat(result1.getId()).isNotEqualTo(result2.getId());
    }

    @Test
    void search_ShouldFilterByUserId() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers")).willReturn(aResponse().withBody("2")));

        paymentService.createPayment(PaymentCreateDto.builder().orderId("order-1").userId("user-A").paymentAmount(new BigDecimal("100.00")).build());
        paymentService.createPayment(PaymentCreateDto.builder().orderId("order-2").userId("user-A").paymentAmount(new BigDecimal("200.00")).build());
        paymentService.createPayment(PaymentCreateDto.builder().orderId("order-3").userId("user-B").paymentAmount(new BigDecimal("300.00")).build());

        await().atMost(10, TimeUnit.SECONDS).until(() -> true); // дать время на Kafka

        Page<PaymentDto> userAPayments = paymentService.search(PageRequest.of(0, 10), null, "user-A", null);

        assertThat(userAPayments.getContent()).hasSize(2)
                .allMatch(p -> p.getUserId().equals("user-A"));
    }

    @Test
    void search_ShouldFilterByOrderId() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers")).willReturn(aResponse().withBody("2")));

        paymentService.createPayment(PaymentCreateDto.builder().orderId("order-1").userId("user-A").paymentAmount(new BigDecimal("100.00")).build());
        paymentService.createPayment(PaymentCreateDto.builder().orderId("order-2").userId("user-A").paymentAmount(new BigDecimal("200.00")).build());

        await().atMost(10, TimeUnit.SECONDS).until(() -> true);

        Page<PaymentDto> order2Payments = paymentService.search(PageRequest.of(0, 10), "order-2", null, null);

        assertThat(order2Payments.getContent()).hasSize(1)
                .extracting(PaymentDto::getOrderId).containsExactly("order-2");
    }

    @Test
    void search_ShouldFilterByStatus() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers")).willReturn(aResponse().withBody("2")));
        paymentService.createPayment(PaymentCreateDto.builder().orderId("order-1").userId("user-A").paymentAmount(new BigDecimal("100.00")).build());

        wireMockServer.stubFor(get(urlPathEqualTo("/integers")).willReturn(aResponse().withBody("1")));
        paymentService.createPayment(PaymentCreateDto.builder().orderId("order-2").userId("user-A").paymentAmount(new BigDecimal("200.00")).build());

        await().atMost(10, TimeUnit.SECONDS).until(() -> true);

        Page<PaymentDto> successfulPayments = paymentService.search(PageRequest.of(0, 10), null, null, List.of(PaymentStatus.SUCCESS));

        assertThat(successfulPayments.getContent()).hasSize(1)
                .allMatch(p -> p.getStatus() == PaymentStatus.SUCCESS);
    }

    @Test
    void search_ShouldReturnPaginatedResults() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers")).willReturn(aResponse().withBody("2")));

        for (int i = 1; i <= 15; i++) {
            paymentService.createPayment(PaymentCreateDto.builder()
                    .orderId("order-" + i)
                    .userId("user")
                    .paymentAmount(new BigDecimal(i * 10.00))
                    .build());
        }

        await().atMost(15, TimeUnit.SECONDS).until(() -> true);

        Page<PaymentDto> page1 = paymentService.search(PageRequest.of(0, 5), null, null, null);

        assertThat(page1.getContent()).hasSize(5);
        assertThat(page1.getTotalElements()).isEqualTo(15);
        assertThat(page1.getTotalPages()).isEqualTo(3);
    }

    @Test
    void getTotalAmount_ShouldSumSuccessfulPaymentsInDateRange() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers")).willReturn(aResponse().withBody("2")));
        PaymentDto p1 = paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-success-1")
                .userId("user-1")
                .paymentAmount(new BigDecimal("150.00"))
                .build());

        wireMockServer.stubFor(get(urlPathEqualTo("/integers")).willReturn(aResponse().withBody("1")));
        paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-failed-1")
                .userId("user-1")
                .paymentAmount(new BigDecimal("250.00"))
                .build());

        wireMockServer.stubFor(get(urlPathEqualTo("/integers")).willReturn(aResponse().withBody("4")));
        PaymentDto p2 = paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-success-2")
                .userId("user-2")
                .paymentAmount(new BigDecimal("350.00"))
                .build());

        await().atMost(10, TimeUnit.SECONDS).until(() -> true);

        LocalDateTime from = p1.getTimestamp().minusHours(1);
        LocalDateTime to = p2.getTimestamp().plusHours(1);

        BigDecimal totalAmount = paymentService.getTotalAmount(from, to);

        assertThat(totalAmount).isEqualTo(new BigDecimal("500.00"));
    }

    @Test
    void getTotalAmount_ShouldReturnZeroForEmptyDateRange() {
        BigDecimal totalAmount = paymentService.getTotalAmount(
                LocalDateTime.now().plusYears(1),
                LocalDateTime.now().plusYears(1).plusDays(1));

        assertThat(totalAmount).isEqualTo(BigDecimal.ZERO);
    }

    @Test
    void getTotalAmount_ShouldReturnZeroForFailedPayments() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers")).willReturn(aResponse().withBody("1")));

        PaymentDto failedPayment = paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-failed")
                .userId("user-failed")
                .paymentAmount(new BigDecimal("999.99"))
                .build());

        await().atMost(10, TimeUnit.SECONDS).until(() -> true);

        BigDecimal totalAmount = paymentService.getTotalAmount(
                failedPayment.getTimestamp().minusHours(1),
                failedPayment.getTimestamp().plusHours(1));

        assertThat(totalAmount).isEqualTo(BigDecimal.ZERO);
    }
}