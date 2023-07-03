package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
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
import org.jose4j.base64url.Base64;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
import java.util.concurrent.TimeUnit;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.withDefaultFilters;
import static io.kroxylicious.test.tester.KroxyliciousTesters.kroxyliciousTester;
import static io.strimzi.kafka.topicenc.kroxylicious.KafkaAssertions.assertSingletonRecordEquals;
import static io.strimzi.kafka.topicenc.kroxylicious.TopicEncryptionContributor.DECRYPT_FETCH;
import static io.strimzi.kafka.topicenc.kroxylicious.Vault.startVaultContainer;
import static io.strimzi.kafka.topicenc.kroxylicious.config.EncryptionModuleConfigurer.getConfiguration;
import static io.strimzi.kafka.topicenc.kroxylicious.config.EncryptionModuleConfigurer.vaultKmsDefinition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertFalse;

@ExtendWith(KafkaClusterExtension.class)
class VaultFetchDecryptionTest {
    public static final String TOPIC_NAME_A = "circle";
    public static final String TOPIC_NAME_B = "square";
    public static final String UNENCRYPTED_VALUE = "unencryptedValue";
    public static final String KMS_NAME = "vault";
    public static final String SECRET_NAMESPACE = "secret";
    private static final Map<String, SecretKey> secretKeys = TestCrypter.uniqueKeyPerKeyReference(Set.of(TOPIC_NAME_A, TOPIC_NAME_B));
    private static final byte[] ENCRYPTED_VALUE_TOPIC_A = TestCrypter.toEncryptedRecordValue(UNENCRYPTED_VALUE.getBytes(UTF_8), secretKeys.get(TOPIC_NAME_A));
    private static final byte[] ENCRYPTED_VALUE_TOPIC_B = TestCrypter.toEncryptedRecordValue(UNENCRYPTED_VALUE.getBytes(UTF_8), secretKeys.get(TOPIC_NAME_B));
    public static Vault.VaultTester vaultTester;

    KafkaCluster cluster;
    private KroxyliciousTester tester;

    private static String base64KeyFor(String topicName) {
        return Base64.encode(secretKeys.get(topicName).getEncoded());
    }

    @BeforeAll
    public static void beforeAll() {
        vaultTester = startVaultContainer(vault -> {
            vault.withSecretInVault(SECRET_NAMESPACE + "/" + TOPIC_NAME_A, TOPIC_NAME_A + "=" + base64KeyFor(TOPIC_NAME_A))
                    .withSecretInVault(SECRET_NAMESPACE + "/" + TOPIC_NAME_B, TOPIC_NAME_B + "=" + base64KeyFor(TOPIC_NAME_B));
        });
    }

    @AfterAll
    public static void afterAll() {
        vaultTester.close();
    }

    @BeforeEach
    public void setup(@TempDir File tempDir) {
        assertFalse(Arrays.equals(secretKeys.get(TOPIC_NAME_A).getEncoded(), secretKeys.get(TOPIC_NAME_B).getEncoded()), "value should be encrypted differently for each topic");
        List<KmsDefinition> definitions = List.of(vaultKmsDefinition(KMS_NAME, vaultTester, vaultTester.token(), SECRET_NAMESPACE));
        List<TopicPolicy> policies = List.of(new TopicPolicy().setTopic(TOPIC_NAME_A).setKeyReference(TOPIC_NAME_A).setKmsName(KMS_NAME), new TopicPolicy().setTopic(TOPIC_NAME_B).setKeyReference(TOPIC_NAME_B).setKmsName(KMS_NAME));
        Map<String, Object> topicEncryptionConfig = getConfiguration(tempDir, definitions, policies);
        tester = kroxyliciousTester(withDefaultFilters(proxy(cluster))
                .addToFilters(new FilterDefinitionBuilder(DECRYPT_FETCH).withConfig(topicEncryptionConfig).build())
        );
    }

    @AfterEach
    public void teardown() {
        tester.close();
    }

    @Test
    public void testFetchDecryption(Admin admin) {
        try (Producer<String, byte[]> producer = tester.producer(Serdes.String(), Serdes.ByteArray(), Map.of());
             Consumer<String, String> consumer = tester.consumer(Serdes.String(), Serdes.String(), Map.of(ConsumerConfig.GROUP_ID_CONFIG, "another-group-id", ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))
        ) {
            admin.createTopics(List.of(new NewTopic(TOPIC_NAME_A, 1, (short) 1), new NewTopic(TOPIC_NAME_B, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(TOPIC_NAME_A, ENCRYPTED_VALUE_TOPIC_A)).get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(TOPIC_NAME_B, ENCRYPTED_VALUE_TOPIC_B)).get(10, TimeUnit.SECONDS);
            assertSingletonRecordEquals(consumer, TOPIC_NAME_A, UNENCRYPTED_VALUE);
            assertSingletonRecordEquals(consumer, TOPIC_NAME_B, UNENCRYPTED_VALUE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}