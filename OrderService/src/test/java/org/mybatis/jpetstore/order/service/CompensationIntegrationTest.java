package org.mybatis.jpetstore.order.service;

import jakarta.servlet.http.HttpSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mybatis.jpetstore.common.domain.LineItem;
import org.mybatis.jpetstore.common.domain.Order;
import org.mybatis.jpetstore.common.grpc.CatalogGrpcClient;
import org.mybatis.jpetstore.order.domain.OrderRetryStatus;
import org.mybatis.jpetstore.order.exception.OrderFailException;
import org.mybatis.jpetstore.order.repository.LineItemRepository;
import org.mybatis.jpetstore.order.repository.OrderRepository;
import org.mybatis.jpetstore.order.repository.SequenceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = {
    "spring.session.store-type=none",
    "eureka.client.enabled=false",
    "spring.cloud.discovery.enabled=false",
    "spring.sql.init.mode=never",
    "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.mybatis.jpetstore.common.config.CommonAutoConfiguration",
    "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer"
})
@EmbeddedKafka(partitions = 1, topics = {"product_compensation"}, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
@ActiveProfiles("test")
public class CompensationIntegrationTest {

    @Autowired
    private OrderService orderService;

    @MockitoBean
    private OrderRepository orderRepository;
    @MockitoBean
    private SequenceRepository sequenceRepository;
    @MockitoBean
    private LineItemRepository lineItemRepository;
    @MockitoBean
    private CatalogGrpcClient catalogGrpcClient;
    @MockitoBean
    private HttpSession session;

    // KafkaTemplate을 Spy로 등록하여 실제 전송 호출을 캡처
    @MockitoSpyBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    @DisplayName("보상 트랜잭션 통합 테스트: 주문 저장 실패 시 KafkaTemplate.send 가 실제로 호출되어야 한다")
    void verifyKafkaTemplateCallOnFailure() throws Exception {
        // Given
        Order order = new Order();
        order.setOrderId(7777);
        List<LineItem> lineItems = new ArrayList<>();
        LineItem item = new LineItem();
        item.setItemId("EST-1");
        item.setQuantity(3);
        lineItems.add(item);
        order.setLineItems(lineItems);

        when(catalogGrpcClient.updateInventoryQuantity(any(), anyInt())).thenReturn(true);
        when(orderRepository.findStatus(anyInt())).thenReturn(Optional.of(new OrderRetryStatus(7777, "unprocessed")));
        
        doThrow(new RuntimeException("DB_SAVE_ERROR")).when(orderRepository).insert(any(Order.class));

        // When
        try {
            orderService.insertOrder(order, session);
        } catch (OrderFailException e) {
            // expected
        }

        // Then: KafkaTemplate.send 가 올바른 토픽과 데이터로 호출되었는지 확인
        // Map 형태의 데이터가 전송되므로 ArgumentMatcher 사용
        verify(kafkaTemplate, timeout(5000).times(1)).send(eq("product_compensation"), argThat(map -> {
            Map<String, Object> param = (Map<String, Object>) map;
            return param.containsKey("EST-1") && param.get("EST-1").toString().equals("3");
        }));
        
        System.out.println("=== VERIFIED: KafkaTemplate.send was called with correct compensation data! ===");
    }
}
