package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.proxy.filter.FilterCreationContext;
import io.kroxylicious.proxy.filter.FilterFactory;

public class ProduceEncryptFilterFactory implements FilterFactory<ProduceEncryptFilter, Void> {

    @Override
    public ProduceEncryptFilter createFilter(FilterCreationContext context, Void configuration) {
        return new ProduceEncryptFilter();
    }

    @Override
    public Class<ProduceEncryptFilter> filterType() {
        return ProduceEncryptFilter.class;
    }

    @Override
    public Class<Void> configType() {
        return Void.class;
    }


}
