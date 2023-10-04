package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.strimzi.kafka.topicenc.kms.KmsDefinition;
import io.strimzi.kafka.topicenc.policy.TopicPolicy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.serialization.Serdes;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.SecretKey;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.withDefaultFilters;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static io.strimzi.kafka.topicenc.kroxylicious.KafkaAssertions.assertSingletonRecordEquals;
import static io.strimzi.kafka.topicenc.kroxylicious.TopicEncryptionContributor.DECRYPT_FETCH;
import static io.strimzi.kafka.topicenc.kroxylicious.config.EncryptionModuleConfigurer.getConfiguration;
import static io.strimzi.kafka.topicenc.kroxylicious.config.EncryptionModuleConfigurer.mockKmsDefinition;
import static io.strimzi.kafka.topicenc.policy.KeyReferenceSource.RECORD_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(KafkaClusterExtension.class)
class RecordKeyFetchDecryptionTest {

    public static final String TOPIC_NAME_A = "apple";
    public static final String TOPIC_NAME_B = "banana";
    public static final String RECORD_KEY_A = "recordKeyA";
    public static final String RECORD_KEY_B = "recordKeyB";
    public static final String UNENCRYPTED_TOPIC = "unencryptedTopic";
    public static final String UNENCRYPTED_VALUE = "unencryptedValue";
    public static final String KMS_NAME = "test";
    private static final short PRE_TOPIC_ID_SCHEMA = (short) 12;
    private static final short POST_TOPIC_ID_SCHEMA = (short) 13;
    private final Map<String, SecretKey> secretKeys = TestCrypter.uniqueKeyPerKeyReference(Set.of(RECORD_KEY_A, RECORD_KEY_B));
    private final byte[] ENCRYPTED_VALUE_RECORD_KEY_A = TestCrypter.toEncryptedRecordValue(UNENCRYPTED_VALUE.getBytes(UTF_8), secretKeys.get(RECORD_KEY_A));
    private final byte[] ENCRYPTED_VALUE_RECORD_KEY_B = TestCrypter.toEncryptedRecordValue(UNENCRYPTED_VALUE.getBytes(UTF_8), secretKeys.get(RECORD_KEY_B));
    KafkaCluster cluster;
    private KroxyliciousTester tester;

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
        FetchResponseData response = (FetchResponseData) responseCompletableFuture.message();
        FetchResponseData.FetchableTopicResponse fetchableTopicResponse = response.responses().get(0);
        responseConsumer.accept(fetchableTopicResponse);
        FetchResponseData.PartitionData partitionData = fetchableTopicResponse.partitions().get(0);
        assertEquals(0, partitionData.partitionIndex());
        MemoryRecords records = (MemoryRecords) partitionData.records();
        Record record = records.records().iterator().next();
        byte[] valueBuffer = new byte[record.valueSize()];
        record.value().get(valueBuffer);
        return new String(valueBuffer, UTF_8);
    }

    private static void createTopics(Admin admin) throws InterruptedException, ExecutionException, TimeoutException {
        admin.createTopics(List.of(new NewTopic(TOPIC_NAME_A, 1, (short) 1),
                new NewTopic(TOPIC_NAME_B, 1, (short) 1),
                new NewTopic(UNENCRYPTED_TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
    }

    @BeforeEach
    public void setup(@TempDir File tempDir) {
        assertFalse(Arrays.equals(ENCRYPTED_VALUE_RECORD_KEY_A, ENCRYPTED_VALUE_RECORD_KEY_B), "value should be encrypted differently for each record key");
        List<KmsDefinition> definitions = List.of(mockKmsDefinition(KMS_NAME, secretKeys));
        List<TopicPolicy> policies = List.of(new TopicPolicy().setTopic(TOPIC_NAME_A).setKeyReferenceSource(RECORD_KEY).setKmsName(KMS_NAME),
                new TopicPolicy().setTopic(TOPIC_NAME_B).setKeyReferenceSource(RECORD_KEY).setKmsName(KMS_NAME));
        Map<String, Object> topicEncryptionConfig = getConfiguration(tempDir, definitions, policies);
        tester = kroxyliciousTester(withDefaultFilters(proxy(cluster))
                .addNewFilter().withType(DECRYPT_FETCH).withConfig(topicEncryptionConfig).endFilter()
        );
    }

    @AfterEach
    public void teardown() {
        tester.close();
    }

    @Test
    void testFetchDecryption(Admin admin) {
        try (
                Producer<String, byte[]> producer = tester.producer(Serdes.String(), Serdes.ByteArray(), Map.of());
                Consumer<String, String> proxyConsumer = tester.consumer(Serdes.String(), Serdes.String(), Map.of(ConsumerConfig.GROUP_ID_CONFIG, "another-group-id", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
        ) {
            createTopics(admin);
            producer.send(new ProducerRecord<>(TOPIC_NAME_A, RECORD_KEY_A, ENCRYPTED_VALUE_RECORD_KEY_A)).get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(TOPIC_NAME_B, RECORD_KEY_B, ENCRYPTED_VALUE_RECORD_KEY_B)).get(10, TimeUnit.SECONDS);
            assertSingletonRecordEquals(proxyConsumer, TOPIC_NAME_A, UNENCRYPTED_VALUE);
            assertSingletonRecordEquals(proxyConsumer, TOPIC_NAME_B, UNENCRYPTED_VALUE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testTopicWithoutPolicyIsNotDecrypted(Admin admin) {
        try (
                Producer<String, byte[]> producer = tester.producer(Serdes.String(), Serdes.ByteArray(), Map.of());
                Consumer<String, byte[]> proxyConsumer = tester.consumer(Serdes.String(), Serdes.ByteArray(), Map.of(ConsumerConfig.GROUP_ID_CONFIG, "another-group-id", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
        ) {
            createTopics(admin);
            producer.send(new ProducerRecord<>(UNENCRYPTED_TOPIC, ENCRYPTED_VALUE_RECORD_KEY_A)).get(10, TimeUnit.SECONDS);
            KafkaAssertions.assertSingletonRecordEquals(proxyConsumer, UNENCRYPTED_TOPIC, ENCRYPTED_VALUE_RECORD_KEY_A);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testFetchDecryptionWithPreTopicIdFetchRequest(Admin admin) {
        try (Producer<String, byte[]> producer = tester.producer(Serdes.String(), Serdes.ByteArray(), Map.of());
             KafkaClient client = tester.singleRequestClient()
        ) {
            createTopics(admin);
            producer.send(new ProducerRecord<>(TOPIC_NAME_A, RECORD_KEY_A, ENCRYPTED_VALUE_RECORD_KEY_A)).get(10, TimeUnit.SECONDS);
            FetchRequestData message = fetchRequestWith(fetchTopic -> fetchTopic.setTopic(TOPIC_NAME_A));
            Response responseCompletableFuture = client.getSync(new Request(ApiKeys.FETCH, PRE_TOPIC_ID_SCHEMA, "clientId", message));
            String valueString = getOnlyRecordValueFromResponse(
                    fetchableTopicResponse -> assertEquals(TOPIC_NAME_A, fetchableTopicResponse.topic())
                    , responseCompletableFuture);
            assertEquals(UNENCRYPTED_VALUE, valueString);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testFetchDecryptionWithTopicIdFetchRequest(Admin admin) {
        try (Producer<String, byte[]> producer = tester.producer(Serdes.String(), Serdes.ByteArray(), Map.of());
             KafkaClient client = tester.singleRequestClient()
        ) {
            CreateTopicsResult result = admin.createTopics(List.of(new NewTopic(TOPIC_NAME_A, 1, (short) 1)));
            Uuid topicUuid = result.topicId(TOPIC_NAME_A).get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(TOPIC_NAME_A, RECORD_KEY_A, ENCRYPTED_VALUE_RECORD_KEY_A)).get(10, TimeUnit.SECONDS);
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

}