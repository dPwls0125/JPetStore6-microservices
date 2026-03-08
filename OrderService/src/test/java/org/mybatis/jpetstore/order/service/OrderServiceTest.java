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
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
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
    @DisplayName("주문 저장 실패 시 Kafka 보상 메시지를 발행하고 실패 처리한다")
    void shouldCompensateAndFailWhenOrderInsertFails() throws RetryUnknownException {
        Order order = new Order();
        order.setOrderId(1001);
        order.setLineItems(new java.util.ArrayList<>());

        when(orderRepository.findStatus(anyInt())).thenReturn(Optional.empty());
        when(catalogGrpcClient.updateInventoryQuantity(any(), anyInt())).thenReturn(true);
        doThrow(new RuntimeException("DB Insert Error")).when(orderRepository).insert(any(Order.class));

        assertThrows(OrderFailException.class, () -> orderService.insertOrder(order, session));

        verify(kafkaTemplate, times(1)).send(eq("product_compensation"), any());
        verify(orderRepository, times(1)).updateStatus(argThat(status -> status.getStatus().equals("fail")));
    }

    @Test
    @DisplayName("inventory commit 성공 확인 시에도 보상 트랜잭션을 발행하고 실패 처리한다")
    void shouldCompensateAndFailWhenCommitSuccessIsConfirmed() throws RetryUnknownException {
        Order order = new Order();
        order.setOrderId(2002);
        order.setLineItems(new java.util.ArrayList<>());

        when(orderRepository.findStatus(anyInt())).thenReturn(Optional.of(new OrderRetryStatus(2002, "unknown")));
        when(catalogGrpcClient.isInventoryUpdateCommitSuccess(2002)).thenReturn(true);

        assertThrows(OrderFailException.class, () -> orderService.insertOrder(order, session));

        verify(kafkaTemplate, times(1)).send(eq("product_compensation"), any());
        verify(orderRepository, times(1)).updateStatus(argThat(status -> status.getStatus().equals("fail")));
        verify(orderRepository, never()).insert(any(Order.class));
    }

    @Test
    @DisplayName("Chaos 모드에서 주문 저장 실패를 강제로 발생시키고 보상 트랜잭션을 발행한다")
    void shouldForcePersistFailureAndPublishCompensationInChaosMode() {
        Order order = new Order();
        order.setOrderId(3003);
        order.setLineItems(new java.util.ArrayList<>());

        ReflectionTestUtils.setField(orderService, "forcePersistFailure", true);

        when(orderRepository.findStatus(anyInt())).thenReturn(Optional.empty());
        when(catalogGrpcClient.updateInventoryQuantity(any(), anyInt())).thenReturn(true);

        assertThrows(OrderFailException.class, () -> orderService.insertOrder(order, session));

        verify(kafkaTemplate, times(1)).send(eq("product_compensation"), any());
        verify(orderRepository, never()).insert(any(Order.class));
        verify(orderRepository, times(1)).updateStatus(argThat(status -> status.getStatus().equals("fail")));
    }

    @Test
    @DisplayName("Chaos 모드에서 보상 트랜잭션을 생략하면 Kafka 보상 메시지를 발행하지 않는다")
    void shouldSkipCompensationWhenChaosFlagIsEnabled() {
        Order order = new Order();
        order.setOrderId(4004);
        order.setLineItems(new java.util.ArrayList<>());

        ReflectionTestUtils.setField(orderService, "forcePersistFailure", true);
        ReflectionTestUtils.setField(orderService, "skipCompensationOnPersistFailure", true);

        when(orderRepository.findStatus(anyInt())).thenReturn(Optional.empty());
        when(catalogGrpcClient.updateInventoryQuantity(any(), anyInt())).thenReturn(true);
        when(catalogGrpcClient.isInventoryUpdateCommitSuccess(4004)).thenReturn(true);

        assertThrows(OrderFailException.class, () -> orderService.insertOrder(order, session));

        verify(kafkaTemplate, never()).send(eq("product_compensation"), any());
        verify(catalogGrpcClient, times(1)).isInventoryUpdateCommitSuccess(4004);
        verify(orderRepository, times(1)).updateStatus(argThat(status -> status.getStatus().equals("fail")));
    }
}
