package com.chatting.kafkatest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

@RestController
@RequiredArgsConstructor
@Slf4j
public class TestController {

    public final KafkaTemplate<String, String> kafkaTemplate;
    public final ObjectMapper objectMapper;

    @GetMapping("/test")
    public void test() throws JsonProcessingException {
        TestDto test = TestDto.builder()
                .id(1L)
                .username("test")
                .nickname("good")
                .build();

        String message = objectMapper.writeValueAsString(test);
        CompletableFuture<SendResult<String, String>> feature = kafkaTemplate.send("topic", "key: test", message);

        feature.thenAccept(result -> {
            log.info("[PRO] Offset: {}, Partition: {}", result.getRecordMetadata().offset(), result.getRecordMetadata().partition());
        }).exceptionally(ex -> {
            log.error(ex.getMessage());
            return null;
        });
    }

    @KafkaListener(topics = "topic", groupId = "group1")
    public void listen(@Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment ack,
                       @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
                       String test) throws JsonProcessingException {
        log.info("[CON] Message: {}, Key, {}, Partition: {}, Offset: {}, Timestamp: {}", test, key, partition, offset, timestamp);

        TestDto test1 = objectMapper.readValue(test, TestDto.class);
        System.out.println(test1.id);
        System.out.println(test1.username);
        System.out.println(test1.nickname);

        if(true) throw new RuntimeException("Test");

        try {
            Thread.sleep(5000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            ack.acknowledge();
            log.info("end sleep");
        }
    }

    @KafkaListener(topics = "error", groupId = "group1")
    public void error(@Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment ack,
                       @Header(value = KafkaHeaders.RECEIVED_KEY, required = false) String key,
                       @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                       @Header(KafkaHeaders.OFFSET) long offset,
                       @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
                       String message) {
        log.info("[ERROR CON] Message: {}, Key, {}, Partition: {}, Offset: {}, Timestamp: {}", message, key, partition, offset, timestamp);
        ack.acknowledge();
    }
}
