package io.strimzi.kafka.topicenc.kroxylicious.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.strimzi.kafka.topicenc.kms.KmsDefinition;
import io.strimzi.kafka.topicenc.kroxylicious.Vault;
import io.strimzi.kafka.topicenc.kroxylicious.kms.MockKms;
import io.strimzi.kafka.topicenc.policy.TopicPolicy;

import javax.crypto.SecretKey;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.strimzi.kafka.topicenc.kroxylicious.InMemoryPolicyRepositoryConfig.KMS_DEFINITIONS_FILE_PROP_NAME;
import static io.strimzi.kafka.topicenc.kroxylicious.InMemoryPolicyRepositoryConfig.TOPIC_POLICIES_FILE_PROP_NAME;
import static io.strimzi.kafka.topicenc.kroxylicious.TopicEncryptionConfig.IN_MEMORY_POLICY_REPOSITORY_PROP_NAME;
import static io.strimzi.kafka.topicenc.kroxylicious.kms.MockKmsFactory.MOCK_KMS_NAME;
import static java.nio.charset.StandardCharsets.UTF_8;

public class EncryptionModuleConfigurer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    record TestTopicPolicy(String topic, String keyReference, String kmsName) {

    }

    public static Map<String, Object> getConfiguration(File tempDir, List<KmsDefinition> kmsDefinitions, List<TopicPolicy> policies) {
        File topicPolicies = writeTopicPolicyFile(tempDir, policies);
        File kmsDefinitionFile = writeKmsDefinitionFile(tempDir, kmsDefinitions);
        Map<String, Object> configFiles = Map.of(KMS_DEFINITIONS_FILE_PROP_NAME, kmsDefinitionFile.getPath(), TOPIC_POLICIES_FILE_PROP_NAME, topicPolicies.getPath());
        return Map.of(IN_MEMORY_POLICY_REPOSITORY_PROP_NAME, configFiles);
    }


    private static File writeTopicPolicyFile(File tempDir, List<TopicPolicy> policies) {
        try {
            File file = new File(tempDir, "topicPolicies-" + UUID.randomUUID() + ".json");
            // cannot serialize the real TopicPolicy class because it serializes some unexpected fields like isWildcard
            // that cannot be deserialized
            Set<TestTopicPolicy> policiesToWrite = policies.stream().map(topicPolicy -> new TestTopicPolicy(topicPolicy.getTopic(), topicPolicy.getKeyReference(), topicPolicy.getKmsName())).collect(Collectors.toSet());
            Files.writeString(file.toPath(), MAPPER.writeValueAsString(policiesToWrite), UTF_8);
            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static File writeKmsDefinitionFile(File tempDir, List<KmsDefinition> definitions) {
        try {
            File file = new File(tempDir, "kmsDef-" + UUID.randomUUID() + ".json");
            Files.writeString(file.toPath(), MAPPER.writeValueAsString(definitions), UTF_8);
            return file;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static KmsDefinition vaultKmsDefinition(String kmsName, Vault.VaultTester tester, String vaultToken, String namespace) {
        return new KmsDefinition().setName(kmsName).setType("io.strimzi.kafka.topicenc.kms.vault.VaultKmsFactory").setCredential(vaultToken).setUri(URI.create(tester.vault().getHttpHostAddress() + "/v1/" + namespace + "/data"));
    }

    public static KmsDefinition mockKmsDefinition(String kmsName, Map<String, SecretKey> keyReferenceToSecret) {
        return new KmsDefinition().setName(kmsName).setType(MOCK_KMS_NAME).setCredential(MockKms.encodeCredentialString(keyReferenceToSecret));
    }

}
