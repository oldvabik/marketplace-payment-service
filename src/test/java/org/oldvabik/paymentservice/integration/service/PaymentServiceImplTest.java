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

        int wireMockPort = wireMockServer.port();

        String randomApiUrl = "http://localhost:" + wireMockPort + "/integers?num=1&min=1&max=100&col=1&base=10&format=plain";
        registry.add("random.api.url", () -> randomApiUrl);

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
        wireMockServer.resetAll();
    }

    private List<ConsumerRecord<String, CreatePaymentEvent>> pollKafkaRecords(Duration timeout) {
        ConsumerRecords<String, CreatePaymentEvent> records = kafkaConsumer.poll(timeout);
        return StreamSupport.stream(records.spliterator(), false)
                .collect(Collectors.toList());
    }

    @Test
    void createPayment_ShouldCreateSuccessfulPaymentAndSendKafkaEvent() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .withQueryParam("num", equalTo("1"))
                .withQueryParam("min", equalTo("1"))
                .withQueryParam("max", equalTo("100"))
                .withQueryParam("col", equalTo("1"))
                .withQueryParam("base", equalTo("10"))
                .withQueryParam("format", equalTo("plain"))
                .willReturn(aResponse()
                        .withHeader("Content-Type", "text/plain")
                        .withBody("42")));

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

        List<ConsumerRecord<String, CreatePaymentEvent>> records = pollKafkaRecords(Duration.ofSeconds(5));
        kafkaConsumer.commitSync();

        assertThat(records).hasSize(1);
        ConsumerRecord<String, CreatePaymentEvent> record = records.get(0);
        assertThat(record.key()).isEqualTo("order-123");
        assertThat(record.value()).isNotNull();
        assertThat(record.value().getOrderId()).isEqualTo("order-123");
        assertThat(record.value().getPaymentId()).isEqualTo(result.getId());
        assertThat(record.value().getStatus()).isEqualTo("SUCCESS");
        assertThat(record.value().getPaymentAmount()).isEqualTo(new BigDecimal("99.99"));
    }

    @Test
    void createPayment_ShouldCreateFailedPaymentWhenRandomNumberIsOdd() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .withQueryParam("num", equalTo("1"))
                .withQueryParam("min", equalTo("1"))
                .withQueryParam("max", equalTo("100"))
                .withQueryParam("col", equalTo("1"))
                .withQueryParam("base", equalTo("10"))
                .withQueryParam("format", equalTo("plain"))
                .willReturn(aResponse().withBody("1")));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-failed")
                .userId("user-failed")
                .paymentAmount(new BigDecimal("50.00"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isEqualTo(PaymentStatus.FAILED);
    }

    @Test
    void createPayment_ShouldUseFallbackWhenApiReturnsError() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .withQueryParam("num", equalTo("1"))
                .withQueryParam("min", equalTo("1"))
                .withQueryParam("max", equalTo("100"))
                .withQueryParam("col", equalTo("1"))
                .withQueryParam("base", equalTo("10"))
                .withQueryParam("format", equalTo("plain"))
                .willReturn(aResponse().withStatus(500)));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-fallback")
                .userId("user-fallback")
                .paymentAmount(new BigDecimal("50.00"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result).isNotNull();
        assertThat(result.getOrderId()).isEqualTo("order-fallback");
        assertThat(result.getStatus()).isIn(PaymentStatus.SUCCESS, PaymentStatus.FAILED);
    }

    @Test
    void createPayment_ShouldUseFallbackWhenApiTimesOut() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .withQueryParam("num", equalTo("1"))
                .withQueryParam("min", equalTo("1"))
                .withQueryParam("max", equalTo("100"))
                .withQueryParam("col", equalTo("1"))
                .withQueryParam("base", equalTo("10"))
                .withQueryParam("format", equalTo("plain"))
                .willReturn(aResponse()
                        .withFixedDelay(3000)
                        .withBody("10")));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-timeout")
                .userId("user-timeout")
                .paymentAmount(new BigDecimal("25.00"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isIn(PaymentStatus.SUCCESS, PaymentStatus.FAILED);
    }

    @Test
    void createPayment_ShouldUseFallbackForInvalidApiResponse() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .withQueryParam("num", equalTo("1"))
                .withQueryParam("min", equalTo("1"))
                .withQueryParam("max", equalTo("100"))
                .withQueryParam("col", equalTo("1"))
                .withQueryParam("base", equalTo("10"))
                .withQueryParam("format", equalTo("plain"))
                .willReturn(aResponse().withBody("not-a-number")));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-invalid")
                .userId("user-invalid")
                .paymentAmount(new BigDecimal("75.00"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isIn(PaymentStatus.SUCCESS, PaymentStatus.FAILED);
    }

    @Test
    void createPayment_ShouldUseFallbackForEmptyApiResponse() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .withQueryParam("num", equalTo("1"))
                .withQueryParam("min", equalTo("1"))
                .withQueryParam("max", equalTo("100"))
                .withQueryParam("col", equalTo("1"))
                .withQueryParam("base", equalTo("10"))
                .withQueryParam("format", equalTo("plain"))
                .willReturn(aResponse().withBody("")));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-empty")
                .userId("user-empty")
                .paymentAmount(new BigDecimal("88.88"))
                .build();

        PaymentDto result = paymentService.createPayment(paymentCreateDto);

        assertThat(result).isNotNull();
        assertThat(result.getStatus()).isIn(PaymentStatus.SUCCESS, PaymentStatus.FAILED);
    }

    @Test
    void createPayment_ShouldSendKafkaEventAsync() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .withQueryParam("num", equalTo("1"))
                .withQueryParam("min", equalTo("1"))
                .withQueryParam("max", equalTo("100"))
                .withQueryParam("col", equalTo("1"))
                .withQueryParam("base", equalTo("10"))
                .withQueryParam("format", equalTo("plain"))
                .willReturn(aResponse().withBody("2")));

        PaymentCreateDto paymentCreateDto = PaymentCreateDto.builder()
                .orderId("order-kafka")
                .userId("user-kafka")
                .paymentAmount(new BigDecimal("123.45"))
                .build();

        PaymentDto payment = paymentService.createPayment(paymentCreateDto);

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<ConsumerRecord<String, CreatePaymentEvent>> records =
                            pollKafkaRecords(Duration.ofMillis(100));
                    kafkaConsumer.commitSync();

                    assertThat(records).isNotEmpty();
                    assertThat(records).anyMatch(record ->
                            record.value() != null &&
                                    record.value().getPaymentId().equals(payment.getId()));
                });
    }

    @Test
    void createPayment_MultiplePayments_ShouldGenerateDifferentIds() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .withQueryParam("num", equalTo("1"))
                .withQueryParam("min", equalTo("1"))
                .withQueryParam("max", equalTo("100"))
                .withQueryParam("col", equalTo("1"))
                .withQueryParam("base", equalTo("10"))
                .withQueryParam("format", equalTo("plain"))
                .willReturn(aResponse().withBody("2")));

        PaymentCreateDto paymentCreateDto1 = PaymentCreateDto.builder()
                .orderId("order-1")
                .userId("user-1")
                .paymentAmount(new BigDecimal("10.00"))
                .build();

        PaymentCreateDto paymentCreateDto2 = PaymentCreateDto.builder()
                .orderId("order-2")
                .userId("user-1")
                .paymentAmount(new BigDecimal("20.00"))
                .build();

        PaymentDto result1 = paymentService.createPayment(paymentCreateDto1);
        PaymentDto result2 = paymentService.createPayment(paymentCreateDto2);

        assertThat(result1.getId()).isNotEqualTo(result2.getId());
    }

    @Test
    void search_ShouldFilterByUserId() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("2")));

        paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-1")
                .userId("user-A")
                .paymentAmount(new BigDecimal("100.00"))
                .build());

        paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-2")
                .userId("user-A")
                .paymentAmount(new BigDecimal("200.00"))
                .build());

        paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-3")
                .userId("user-B")
                .paymentAmount(new BigDecimal("300.00"))
                .build());

        pollKafkaRecords(Duration.ofSeconds(1));

        Pageable pageable = PageRequest.of(0, 10);
        Page<PaymentDto> userAPayments = paymentService.search(pageable, null, "user-A", null);

        assertThat(userAPayments.getContent()).hasSize(2);
        assertThat(userAPayments.getContent())
                .allMatch(p -> p.getUserId().equals("user-A"));
    }

    @Test
    void search_ShouldFilterByOrderId() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("2")));

        paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-1")
                .userId("user-A")
                .paymentAmount(new BigDecimal("100.00"))
                .build());

        paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-2")
                .userId("user-A")
                .paymentAmount(new BigDecimal("200.00"))
                .build());

        pollKafkaRecords(Duration.ofSeconds(1));

        Pageable pageable = PageRequest.of(0, 10);
        Page<PaymentDto> order2Payments = paymentService.search(pageable, "order-2", null, null);

        assertThat(order2Payments.getContent()).hasSize(1);
        assertThat(order2Payments.getContent().get(0).getOrderId()).isEqualTo("order-2");
    }

    @Test
    void search_ShouldFilterByStatus() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("2")));

        paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-1")
                .userId("user-A")
                .paymentAmount(new BigDecimal("100.00"))
                .build());

        wireMockServer.resetAll();
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("1")));

        paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-2")
                .userId("user-A")
                .paymentAmount(new BigDecimal("200.00"))
                .build());

        pollKafkaRecords(Duration.ofSeconds(1));

        Pageable pageable = PageRequest.of(0, 10);
        Page<PaymentDto> successfulPayments = paymentService.search(pageable, null, null,
                List.of(PaymentStatus.SUCCESS));

        assertThat(successfulPayments.getContent()).hasSize(1);
        assertThat(successfulPayments.getContent().get(0).getStatus()).isEqualTo(PaymentStatus.SUCCESS);
    }

    @Test
    void search_ShouldReturnPaginatedResults() {
        for (int i = 1; i <= 15; i++) {
            wireMockServer.resetAll();
            wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                    .willReturn(aResponse().withBody("2")));

            paymentService.createPayment(PaymentCreateDto.builder()
                    .orderId("order-" + i)
                    .userId("user")
                    .paymentAmount(new BigDecimal(i * 10.00))
                    .build());
        }

        pollKafkaRecords(Duration.ofSeconds(1));

        Pageable firstPage = PageRequest.of(0, 5);
        Page<PaymentDto> page1 = paymentService.search(firstPage, null, null, null);

        assertThat(page1.getContent()).hasSize(5);
        assertThat(page1.getTotalElements()).isEqualTo(15);
        assertThat(page1.getTotalPages()).isEqualTo(3);
        assertThat(page1.isFirst()).isTrue();
        assertThat(page1.isLast()).isFalse();
    }

    @Test
    void getTotalAmount_ShouldSumSuccessfulPaymentsInDateRange() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("2")));

        PaymentDto successfulPayment = paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-success-1")
                .userId("user-1")
                .paymentAmount(new BigDecimal("150.00"))
                .build());

        wireMockServer.resetAll();
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("1")));

        paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-failed-1")
                .userId("user-1")
                .paymentAmount(new BigDecimal("250.00"))
                .build());

        wireMockServer.resetAll();
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("4")));

        PaymentDto successfulPayment2 = paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-success-2")
                .userId("user-2")
                .paymentAmount(new BigDecimal("350.00"))
                .build());

        pollKafkaRecords(Duration.ofSeconds(1));

        LocalDateTime from = successfulPayment.getTimestamp().minusHours(1);
        LocalDateTime to = successfulPayment2.getTimestamp().plusHours(1);

        BigDecimal totalAmount = paymentService.getTotalAmount(from, to);

        assertThat(totalAmount).isEqualTo(new BigDecimal("500.00"));
    }

    @Test
    void getTotalAmount_ShouldReturnZeroForEmptyDateRange() {
        LocalDateTime futureDate = LocalDateTime.now().plusYears(1);
        LocalDateTime furtherFuture = futureDate.plusDays(1);

        BigDecimal totalAmount = paymentService.getTotalAmount(futureDate, furtherFuture);

        assertThat(totalAmount).isEqualTo(BigDecimal.ZERO);
    }

    @Test
    void getTotalAmount_ShouldReturnZeroForFailedPayments() {
        wireMockServer.stubFor(get(urlPathEqualTo("/integers"))
                .willReturn(aResponse().withBody("1")));

        PaymentDto failedPayment = paymentService.createPayment(PaymentCreateDto.builder()
                .orderId("order-failed")
                .userId("user-failed")
                .paymentAmount(new BigDecimal("999.99"))
                .build());

        pollKafkaRecords(Duration.ofSeconds(1));

        LocalDateTime from = failedPayment.getTimestamp().minusHours(1);
        LocalDateTime to = failedPayment.getTimestamp().plusHours(1);

        BigDecimal totalAmount = paymentService.getTotalAmount(from, to);

        assertThat(totalAmount).isEqualTo(BigDecimal.ZERO);
    }
}