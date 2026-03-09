package org.mybatis.jpetstore.catalog.service;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mybatis.jpetstore.catalog.CatalogServiceApplication;
import org.mybatis.jpetstore.catalog.controller.CatalogController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.boot.test.mock.mockito.SpyBean;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Saga 패턴의 보상 트랜잭션 수신 측(Catalog Service) 통합 테스트입니다.
 */
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
    "grpc.server.port=0"
})
@EmbeddedKafka(partitions = 1, topics = {"product_compensation"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@ActiveProfiles("test")
public class RollbackIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @SpyBean
    private CatalogService catalogService;

    @Autowired
    private CatalogController catalogController;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @BeforeEach
    public void setUp() {
        for (MessageListenerContainer messageListenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @Test
    @DisplayName("보상 트랜잭션 통합 테스트: Catalog 서비스는 Kafka 보상 이벤트를 받으면 실제 DB 재고를 복구해야 한다")
    void verifyCatalogRollbackByKafkaEvent() {
        // Given: 보상 이벤트 데이터 (아이템 EST-1, 수량 10개 복구)
        Map<String, Object> data = new HashMap<>();
        data.put("EST-1", 10);

        // 테스트 시작 전 EST-1 의 현재 재고 확인 (데이터로드 SQL 에 의해 10000개로 초기화됨)
        Integer initialQuantity = catalogService.getItemQuantity("EST-1");
        assertNotNull(initialQuantity);
        System.out.println("=== [TEST] Initial DB Quantity for EST-1: " + initialQuantity + " ===");

        // When: 실제 Kafka 토픽으로 메시지 전송
        System.out.println("=== [TEST] SENDING COMPENSATION EVENT TO EMBEDDED KAFKA ===");
        kafkaTemplate.send("product_compensation", data);
        kafkaTemplate.flush();

        // Then: CatalogController의 @KafkaListener가 동작하여 catalogService.rollbackInventory를 호출하는지 대기
        verify(catalogService, timeout(15000).times(1)).rollbackInventory(any());
        
        // Kafka 메시지 처리로 인해 트랜잭션이 커밋되는 시간을 약간 대기 (필요시)
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // DB 재고가 실제로 복구되었는지 확인
        Integer updatedQuantity = catalogService.getItemQuantity("EST-1");
        System.out.println("=== [TEST] Updated DB Quantity for EST-1: " + updatedQuantity + " ===");
        
        assertEquals(initialQuantity + 10, updatedQuantity);
        
        System.out.println("=== [TEST] VERIFIED: Catalog Service DB Rollback Triggered and Executed by REAL Kafka Event! ===");
    }
}
