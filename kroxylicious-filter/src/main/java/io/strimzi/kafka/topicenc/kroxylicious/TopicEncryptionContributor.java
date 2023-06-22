package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.proxy.filter.FilterContributor;
import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.service.BaseContributor;

public class TopicEncryptionContributor extends BaseContributor<KrpcFilter> implements FilterContributor {

    public static final String DECRYPT_FETCH = "TopicEncryption::DecryptFetch";
    public static final String ENCRYPT_PRODUCE = "TopicEncryption::EncryptProduce";
    public static final BaseContributorBuilder<KrpcFilter> FILTERS = BaseContributor.<KrpcFilter>builder()
            .add(DECRYPT_FETCH, TopicEncryptionConfig.class, FetchDecryptFilter::new)
            .add(ENCRYPT_PRODUCE, TopicEncryptionConfig.class, ProduceEncryptFilter::new);

    public TopicEncryptionContributor() {
        super(FILTERS);
    }

}
