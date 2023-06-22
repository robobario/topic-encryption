package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.strimzi.kafka.topicenc.kms.KmsDefinition;
import io.strimzi.kafka.topicenc.policy.TopicPolicy;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
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
import static io.strimzi.kafka.topicenc.kroxylicious.TopicEncryptionContributor.ENCRYPT_PRODUCE;
import static io.strimzi.kafka.topicenc.kroxylicious.config.EncryptionModuleConfigurer.getConfiguration;
import static io.strimzi.kafka.topicenc.kroxylicious.config.EncryptionModuleConfigurer.mockKmsDefinition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(KafkaClusterExtension.class)
class ProduceEncryptionTest {
    public static final String TOPIC_NAME_A = "apple";
    public static final String TOPIC_NAME_B = "banana";
    public static final String UNENCRYPTED_TOPIC = "unencryptedTopic";
    public static final String UNENCRYPTED_VALUE = "unencryptedValue";
    public static final String KMS_NAME = "test";
    private final Map<String, SecretKey> secretKeys = TestCrypter.uniqueKeyPerKeyReference(Set.of(TOPIC_NAME_A, TOPIC_NAME_B));
    KafkaCluster cluster;
    private KroxyliciousTester tester;

    private static void createTopics(Admin admin) throws InterruptedException, ExecutionException, TimeoutException {
        admin.createTopics(List.of(new NewTopic(TOPIC_NAME_A, 1, (short) 1),
                new NewTopic(TOPIC_NAME_B, 1, (short) 1),
                new NewTopic(UNENCRYPTED_TOPIC, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
    }

    @BeforeEach
    public void setup(@TempDir File tempDir) {
        assertFalse(Arrays.equals(secretKeys.get(TOPIC_NAME_A).getEncoded(), secretKeys.get(TOPIC_NAME_B).getEncoded()), "value should be encrypted differently for each topic");
        List<KmsDefinition> definitions = List.of(mockKmsDefinition(KMS_NAME, secretKeys));
        List<TopicPolicy> policies = List.of(new TopicPolicy().setTopic(TOPIC_NAME_A).setKeyReference(TOPIC_NAME_A).setKmsName(KMS_NAME),
                new TopicPolicy().setTopic(TOPIC_NAME_B).setKeyReference(TOPIC_NAME_B).setKmsName(KMS_NAME));
        Map<String, Object> topicEncryptionConfig = getConfiguration(tempDir, definitions, policies);
        tester = kroxyliciousTester(withDefaultFilters(proxy(cluster))
                .addNewFilter().withType(ENCRYPT_PRODUCE).withConfig(topicEncryptionConfig).endFilter()
        );
    }

    @AfterEach
    public void teardown() {
        tester.close();
    }

    @Test
    public void testFetchDecryption(Admin admin) {
        try (
                Producer<String, String> producer = tester.producer();
                Consumer<String, byte[]> proxyConsumer = tester.consumer(Serdes.String(), Serdes.ByteArray(), Map.of(ConsumerConfig.GROUP_ID_CONFIG, "another-group-id", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
        ) {
            createTopics(admin);
            producer.send(new ProducerRecord<>(TOPIC_NAME_A, UNENCRYPTED_VALUE)).get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(TOPIC_NAME_B, UNENCRYPTED_VALUE)).get(10, TimeUnit.SECONDS);
            assertSingletonRecordEquals(proxyConsumer, TOPIC_NAME_A, (s) -> decrypt(s, TOPIC_NAME_A), UNENCRYPTED_VALUE);
            assertSingletonRecordEquals(proxyConsumer, TOPIC_NAME_B, (s) -> decrypt(s, TOPIC_NAME_B), UNENCRYPTED_VALUE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull
    private String decrypt(byte[] s, String topicName) {
        return new String(TestCrypter.toDecryptedRecordValue(s, secretKeys.get(topicName)), UTF_8);
    }

    @Test
    public void testTopicWithoutPolicyIsNotEncrypted(Admin admin) {
        try (
                Producer<String, String> producer = tester.producer();
                Consumer<String, String> proxyConsumer = tester.consumer()
        ) {
            createTopics(admin);
            producer.send(new ProducerRecord<>(UNENCRYPTED_TOPIC, UNENCRYPTED_VALUE)).get(10, TimeUnit.SECONDS);
            assertSingletonRecordEquals(proxyConsumer, UNENCRYPTED_TOPIC, UNENCRYPTED_VALUE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}