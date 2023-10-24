package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(KafkaClusterExtension.class)
class TopicEncryptionTest {

    private static final short PRE_TOPIC_ID_SCHEMA = (short) 12;
    private static final short POST_TOPIC_ID_SCHEMA = (short) 13;
    public static final String TOPIC_NAME = "example";
    public static final String UNENCRYPTED_VALUE = "unencryptedValue";

    KafkaCluster cluster;
    private KroxyliciousTester tester;

    @BeforeEach
    public void setup() {
        tester = kroxyliciousTester(proxy(cluster)
                .addToFilters(new FilterDefinitionBuilder(ProduceEncryptFilter.class.getSimpleName()).build())
                .addToFilters(new FilterDefinitionBuilder(FetchDecryptFilter.class.getSimpleName()).build())
        );
    }

    @AfterEach
    public void teardown() {
        tester.close();
    }

    @Test
    public void testEncryptionRoundtrip(Admin admin) {
        try (Producer<String, String> producer = tester.producer();
             Consumer<String, byte[]> kafkaClusterConsumer = getConsumer(cluster);
             Consumer<String, byte[]> proxyConsumer = tester.consumer(Serdes.String(), Serdes.ByteArray(), Map.of(ConsumerConfig.GROUP_ID_CONFIG, "another-group-id", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
        ) {
            admin.createTopics(List.of(new NewTopic(TOPIC_NAME, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(TOPIC_NAME, UNENCRYPTED_VALUE)).get(10, TimeUnit.SECONDS);
            ConsumerRecord<String, byte[]> clusterRecord = getOnlyRecord(kafkaClusterConsumer, TOPIC_NAME);
            ConsumerRecord<String, byte[]> proxiedRecord = getOnlyRecord(proxyConsumer, TOPIC_NAME);
            assertFalse(Arrays.equals(clusterRecord.value(), proxiedRecord.value())); // todo check encryption?
            assertEquals(UNENCRYPTED_VALUE, new String(proxiedRecord.value(), StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testEncryptionRoundtripWithPreTopicIdFetchRequest(Admin admin) {
        try (Producer<String, String> producer = tester.producer();
             KafkaClient client = tester.simpleTestClient();
        ) {
            admin.createTopics(List.of(new NewTopic(TOPIC_NAME, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(TOPIC_NAME, UNENCRYPTED_VALUE)).get(10, TimeUnit.SECONDS);
            FetchRequestData message = fetchRequestWith(fetchTopic -> fetchTopic.setTopic(TOPIC_NAME));
            Response responseCompletableFuture = client.getSync(new Request(ApiKeys.FETCH, PRE_TOPIC_ID_SCHEMA, "clientId", message));
            String valueString = getOnlyRecordValueFromResponse(
                    fetchableTopicResponse -> assertEquals(TOPIC_NAME, fetchableTopicResponse.topic())
                    , responseCompletableFuture);
            assertEquals(UNENCRYPTED_VALUE, valueString);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testEncryptionRoundtripWithPostTopicIdFetchRequest(Admin admin) {
        try (Producer<String, String> producer = tester.producer();
             KafkaClient client = tester.simpleTestClient();
        ) {
            CreateTopicsResult result = admin.createTopics(List.of(new NewTopic(TOPIC_NAME, 1, (short) 1)));
            Uuid topicUuid = result.topicId(TOPIC_NAME).get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(TOPIC_NAME, UNENCRYPTED_VALUE)).get(10, TimeUnit.SECONDS);
            FetchRequestData message = fetchRequestWith(fetchTopic -> fetchTopic.setTopicId(topicUuid));
            Response responseCompletableFuture = client.getSync(new Request(ApiKeys.FETCH, POST_TOPIC_ID_SCHEMA, "clientId", message));
            String valueString = getOnlyRecordValueFromResponse(
                    fetchableTopicResponse -> assertEquals(topicUuid, fetchableTopicResponse.topicId())
                    , responseCompletableFuture);
            assertEquals(UNENCRYPTED_VALUE, valueString);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private static FetchRequestData fetchRequestWith(java.util.function.Consumer<FetchRequestData.FetchTopic> func) {
        FetchRequestData message = new FetchRequestData();
        message.setReplicaId(-1);
        message.setMaxWaitMs(5000);
        message.setMinBytes(1);
        message.setMaxBytes(1024);
        message.setIsolationLevel((byte) 0);
        message.setSessionId(0);
        message.setSessionEpoch(0);
        FetchRequestData.FetchTopic topic = new FetchRequestData.FetchTopic();
        func.accept(topic);
        FetchRequestData.FetchPartition fetchPartition = new FetchRequestData.FetchPartition();
        fetchPartition.setPartition(0);
        topic.setPartitions(List.of(fetchPartition));
        message.setTopics(List.of(topic));
        return message;
    }

    @NotNull
    private static String getOnlyRecordValueFromResponse(java.util.function.Consumer<FetchResponseData.FetchableTopicResponse> responseConsumer, Response responseCompletableFuture) {
        FetchResponseData response = (FetchResponseData) responseCompletableFuture.payload().message();
        FetchResponseData.FetchableTopicResponse fetchableTopicResponse = response.responses().get(0);
        responseConsumer.accept(fetchableTopicResponse);
        FetchResponseData.PartitionData partitionData = fetchableTopicResponse.partitions().get(0);
        assertEquals(0, partitionData.partitionIndex());
        MemoryRecords records = (MemoryRecords) partitionData.records();
        Record record = records.records().iterator().next();
        byte[] valueBuffer = new byte[record.valueSize()];
        record.value().get(valueBuffer);
        return new String(valueBuffer, StandardCharsets.UTF_8);
    }

    private static ConsumerRecord<String, byte[]> getOnlyRecord(Consumer<String, byte[]> kafkaClusterConsumer, String topic) {
        kafkaClusterConsumer.subscribe(List.of(topic));
        ConsumerRecords<String, byte[]> poll = kafkaClusterConsumer.poll(Duration.ofSeconds(10));
        if (poll.count() != 1) {
            fail("expected to poll exactly one record from Kafka, received " + poll.count());
        }
        Iterable<ConsumerRecord<String, byte[]>> records = poll.records(topic);
        Iterator<ConsumerRecord<String, byte[]>> iterator = records.iterator();
        return iterator.next();
    }

    @NotNull
    private static KafkaConsumer<String, byte[]> getConsumer(KafkaCluster cluster) {
        HashMap<String, Object> config = new HashMap<>(cluster.getKafkaClientConfiguration());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(config);
    }

}