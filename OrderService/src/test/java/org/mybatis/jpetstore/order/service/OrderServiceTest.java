package org.mybatis.jpetstore.order.service;

import jakarta.servlet.http.HttpSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mybatis.jpetstore.common.domain.Order;
import org.mybatis.jpetstore.common.grpc.CatalogGrpcClient;
import org.mybatis.jpetstore.order.domain.OrderRetryStatus;
import org.mybatis.jpetstore.order.exception.OrderFailException;
import org.mybatis.jpetstore.order.exception.RetryUnknownException;
import org.mybatis.jpetstore.order.repository.OrderRepository;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private CatalogGrpcClient catalogGrpcClient;

    @Mock
    private HttpSession session;

    @InjectMocks
    private OrderService orderService;

    @Test
    @DisplayName("주문 저장 실패 시 Kafka 보상 메시지가 발행되는지 확인")
    void shouldSendKafkaMessageWhenOrderInsertFails() throws OrderFailException, RetryUnknownException {
        // 1. 테스트 데이터 준비
        Order order = new Order();
        order.setOrderId(1001);
        order.setLineItems(new java.util.ArrayList<>());

        // 2. Mock 설정
        when(orderRepository.findStatus(anyInt()))
            .thenReturn(Optional.empty())
            .thenReturn(Optional.of(new OrderRetryStatus(1001, "unprocessed")));
        
        when(catalogGrpcClient.updateInventoryQuantity(any(), anyInt())).thenReturn(true);

        // 핵심: orderRepository.insert 호출 시 강제로 예외 발생
        doThrow(new RuntimeException("DB Insert Error")).when(orderRepository).insert(any(Order.class));

        // 3. 테스트 실행
        orderService.insertOrder(order, session);

        // 4. 검증: kafkaTemplate.send가 "product_compensation" 토픽으로 호출되었는지 확인
        verify(kafkaTemplate, times(1)).send(eq("product_compensation"), any());
        
        // 추가 검증: 상태가 success로 업데이트 되었는지 확인
        verify(orderRepository, times(1)).updateStatus(argThat(status -> status.getStatus().equals("success")));
    }
}
