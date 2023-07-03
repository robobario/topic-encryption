package io.strimzi.kafka.topicenc.kroxylicious;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.kroxylicious.proxy.config.BaseConfig;
import io.strimzi.kafka.topicenc.policy.PolicyRepository;

import java.util.Objects;

public class TopicEncryptionConfig extends BaseConfig {

    public static final String IN_MEMORY_POLICY_REPOSITORY_PROP_NAME = "inMemoryPolicyRepository";
    private final InMemoryPolicyRepositoryConfig inMemoryPolicyRepository;

    @JsonCreator
    public TopicEncryptionConfig(@JsonProperty(value = IN_MEMORY_POLICY_REPOSITORY_PROP_NAME) InMemoryPolicyRepositoryConfig inMemoryPolicyRepository) {
        this.inMemoryPolicyRepository = inMemoryPolicyRepository;
        Objects.requireNonNull(inMemoryPolicyRepository, "Currently " + IN_MEMORY_POLICY_REPOSITORY_PROP_NAME
                + " configuration is required as it is the only PolicyRepository implementation");
    }

    public PolicyRepository getPolicyRepository() {
        return inMemoryPolicyRepository.getPolicyRepository();
    }
}
