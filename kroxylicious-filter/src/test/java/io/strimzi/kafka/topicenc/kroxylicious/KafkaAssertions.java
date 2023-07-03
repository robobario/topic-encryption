package io.strimzi.kafka.topicenc.kroxylicious;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaAssertions {

    public static void assertSingletonRecordEquals(Consumer<String, byte[]> kafkaClusterConsumer, String topic, byte[] expected) {
        ConsumerRecord<String, byte[]> onlyRecord = getSingletonRecord(kafkaClusterConsumer, topic);
        assertArrayEquals(expected, onlyRecord.value(), "only record value in " + topic + " did not match expectation");
    }

    public static void assertSingletonRecordEquals(Consumer<String, byte[]> kafkaClusterConsumer, String topic, Function<byte[], String> transform, String expected) {
        ConsumerRecord<String, byte[]> onlyRecord = getSingletonRecord(kafkaClusterConsumer, topic);
        String transformed = transform.apply(onlyRecord.value());
        assertEquals(expected, transformed, "only record value in " + topic + " did not match expectation");
    }

    public static void assertSingletonRecordEquals(Consumer<String, String> kafkaClusterConsumer, String topic, String expected) {
        ConsumerRecord<String, String> onlyRecord = getSingletonRecord(kafkaClusterConsumer, topic);
        assertEquals(expected, onlyRecord.value(), "only record value in " + topic + " did not match expectation");
    }

    private static <T> ConsumerRecord<String, T> getSingletonRecord(Consumer<String, T> kafkaClusterConsumer, String topic) {
        kafkaClusterConsumer.subscribe(List.of(topic));
        ConsumerRecords<String, T> poll = kafkaClusterConsumer.poll(Duration.ofSeconds(10));
        if (poll.count() != 1) {
            fail("expected to poll exactly one record from Kafka, received " + poll.count());
        }
        Iterable<ConsumerRecord<String, T>> records = poll.records(topic);
        Iterator<ConsumerRecord<String, T>> iterator = records.iterator();
        return iterator.next();
    }

}
