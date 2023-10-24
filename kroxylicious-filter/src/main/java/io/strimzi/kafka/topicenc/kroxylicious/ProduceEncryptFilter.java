package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import io.strimzi.kafka.topicenc.EncryptionModule;
import io.strimzi.kafka.topicenc.kms.KmsDefinition;
import io.strimzi.kafka.topicenc.kms.test.TestKms;
import io.strimzi.kafka.topicenc.policy.InMemoryPolicyRepository;
import io.strimzi.kafka.topicenc.policy.TopicPolicy;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletionStage;

public class ProduceEncryptFilter implements ProduceRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(ProduceEncryptFilter.class);

    private final EncryptionModule module = new EncryptionModule(new InMemoryPolicyRepository(List.of(new TopicPolicy().setTopic(TopicPolicy.ALL_TOPICS).setKms(new TestKms(new KmsDefinition())))));

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
        try {
            for (ProduceRequestData.TopicProduceData topicDatum : request.topicData()) {
                try {
                    module.encrypt(topicDatum);
                } catch (Exception e) {
                    log.error("Failed to encrypt a produceRequest for topic: " + topicDatum.name(), e);
                    throw new RuntimeException(e);
                }
            }
            return context.forwardRequest(header, request);
        } catch (Exception e) {
            return sendErrorProduceResponse(request, context);
        }
    }

    private static CompletionStage<RequestFilterResult> sendErrorProduceResponse(ProduceRequestData request, FilterContext context) {
        ProduceResponseData response = new ProduceResponseData();
        for (ProduceRequestData.TopicProduceData topicDatum : request.topicData()) {
            ProduceResponseData.TopicProduceResponse topicResponse = new ProduceResponseData.TopicProduceResponse();
            topicResponse.setName(topicDatum.name());
            for (ProduceRequestData.PartitionProduceData partitionDatum : topicDatum.partitionData()) {
                ProduceResponseData.PartitionProduceResponse partitionProduceResponse = new ProduceResponseData.PartitionProduceResponse();
                partitionProduceResponse.setIndex(partitionDatum.index());
                partitionProduceResponse.setErrorMessage(Errors.UNKNOWN_SERVER_ERROR.message());
                partitionProduceResponse.setErrorCode(Errors.UNKNOWN_SERVER_ERROR.code());
                topicResponse.partitionResponses().add(partitionProduceResponse);
            }
            response.responses().add(topicResponse);
        }
        return context.requestFilterResultBuilder().shortCircuitResponse(response).completed();
    }
}
