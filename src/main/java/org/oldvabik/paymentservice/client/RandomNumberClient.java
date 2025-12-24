package org.oldvabik.paymentservice.client;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import java.time.Duration;

@Slf4j
@Component
public class RandomNumberClient {

    private final RestTemplate restTemplate;
    private final String randomApiUrl;

    public RandomNumberClient(
            RestTemplateBuilder builder,
            @Value("${random.api.url}") String randomApiUrl
    ) {
        this.randomApiUrl = randomApiUrl;
        this.restTemplate = builder
                .setConnectTimeout(Duration.ofMillis(2000))
                .setReadTimeout(Duration.ofMillis(2000))
                .build();
    }

    public int getRandomNumber() {
        try {
            log.debug("[RandomNumberClient] Fetching random number from API: {}", randomApiUrl);
            String response = restTemplate.getForObject(randomApiUrl, String.class);

            if (response != null && !response.isBlank()) { // <- проверка на null и пустую строку
                int number = Integer.parseInt(response.trim());
                log.debug("[RandomNumberClient] Got random number: {}", number);
                return number;
            }
        } catch (RestClientException | NumberFormatException e) {
            log.warn("[RandomNumberClient] Failed to get random number from API: {}, using fallback",
                    e.getClass().getSimpleName(), e);
        }

        return getFallbackRandomNumber();
    }


    private int getFallbackRandomNumber() {
        return (int) (Math.random() * 11) - 5;
    }
}
