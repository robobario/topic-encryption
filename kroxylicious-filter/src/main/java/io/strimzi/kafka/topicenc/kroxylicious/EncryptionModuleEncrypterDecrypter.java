package io.strimzi.kafka.topicenc.kroxylicious;

import io.strimzi.kafka.topicenc.EncryptionModule;
import io.strimzi.kafka.topicenc.policy.InMemoryPolicyRepository;
import io.strimzi.kafka.topicenc.policy.JsonPolicyLoader;
import io.strimzi.kafka.topicenc.policy.TopicPolicy;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.ProduceRequestData;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EncryptionModuleEncrypterDecrypter implements EncrypterDecrypter {
    private static final Map<EncryptionModuleConfiguration, EncryptionModuleEncrypterDecrypter> crypters = new HashMap<>();

    private final EncryptionModule encryptionModule;

    public static EncrypterDecrypter getOrCreateInstance(File kmsDefsFile, File policyFile) {
        EncryptionModuleConfiguration config = new EncryptionModuleConfiguration(kmsDefsFile.toPath(), policyFile.toPath());
        return crypters.computeIfAbsent(config, encryptionModuleConfiguration -> new EncryptionModuleEncrypterDecrypter(config));
    }


    record EncryptionModuleConfiguration(Path kmsDefsPath, Path topicPoliciesPath) {
    }

    private EncryptionModuleEncrypterDecrypter(EncryptionModuleConfiguration configuration) {
        try {
            encryptionModule = load(configuration.kmsDefsPath.toFile(), configuration.topicPoliciesPath.toFile());
        } catch (Exception e) {
            throw new RuntimeException("Failed to create topic policies", e);
        }
    }

    private EncryptionModule load(File kmsDefsFile, File policyFile) throws IOException {
        List<TopicPolicy> topicPolicies = JsonPolicyLoader.loadTopicPolicies(kmsDefsFile, policyFile);
        InMemoryPolicyRepository inMemoryPolicyRepository = new InMemoryPolicyRepository(topicPolicies);
        return new EncryptionModule(inMemoryPolicyRepository);
    }

    @Override
    public boolean encrypt(ProduceRequestData.TopicProduceData topicData) {
        try {
            return encryptionModule.encrypt(topicData);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean decrypt(FetchResponseData.FetchableTopicResponse fetchRsp) {
        try {
            return encryptionModule.decrypt(fetchRsp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
