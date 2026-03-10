package org.mybatis.jpetstore.catalog.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mybatis.jpetstore.catalog.CatalogServiceApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest(classes = CatalogServiceApplication.class, properties = {
    "eureka.client.enabled=false",
    "spring.cloud.discovery.enabled=false",
    "spring.cloud.compatibility-verifier.enabled=false",
    "spring.sql.init.mode=always",
    "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.mybatis.jpetstore.common.config.CommonAutoConfiguration",
    "spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer",
    "spring.kafka.consumer.properties.spring.json.trusted.packages=*",
    "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer",
    "kafka.bootstrap-servers=${spring.kafka.bootstrap-servers}",
    "kafka.consumer.concurrency=3", // 3개의 스레드 활성화 (메인 설정 활용)
    "grpc.server.port=0"
})
@EmbeddedKafka(partitions = 3, topics = {"product_compensation"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@ActiveProfiles("test")
public class KafkaConcurrencyTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @SpyBean
    private CatalogService catalogService;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Test
    @DisplayName("성능 입증 테스트: Concurrency=3 일 때 30개의 메시지를 병렬로 처리하는지 확인")
    void measurePerformanceWithMultipleConsumers() throws InterruptedException {
        // 파티션 할당 대기
        for (MessageListenerContainer container : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(container, embeddedKafkaBroker.getPartitionsPerTopic());
        }

        int messageCount = 30;
        CountDownLatch latch = new CountDownLatch(messageCount);
        AtomicInteger processedCount = new AtomicInteger(0);

        doAnswer(invocation -> {
            Thread.sleep(100); // 100ms 지연, consumer가 만약 하나라면 해당 100ms만큼 blocking되는 구조
            processedCount.incrementAndGet();
            System.out.println("[CONSUMER] Thread: " + Thread.currentThread().getName() + " | Total: " + processedCount.get());
            latch.countDown();
            return null;
        }).when(catalogService).rollbackInventory(any());

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < messageCount; i++) {
            Map<String, Object> data = new HashMap<>();
            data.put("EST-" + (i % 5), 1);
            // 키를 다르게 주어 3개 파티션에 골고루 분산
            kafkaTemplate.send("product_compensation", "key-" + i, data);
        }

        boolean completed = latch.await(20, TimeUnit.SECONDS);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        System.out.println("==========================================");
        System.out.println("Message Count: " + messageCount);
        System.out.println("Total Processing Time: " + totalTime + " ms");
        System.out.println("==========================================");

        assert(totalTime < 2500) : "Parallel processing is NOT working. Expected < 2500ms, but got " + totalTime + "ms";
    }
}
