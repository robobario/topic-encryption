package io.strimzi.kafka.topicenc.kroxylicious;

import io.kroxylicious.proxy.filter.FetchResponseFilter;
import io.kroxylicious.proxy.filter.KrpcFilterContext;
import io.kroxylicious.proxy.filter.MetadataResponseFilter;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.strimzi.kafka.topicenc.EncryptionModule;
import io.strimzi.kafka.topicenc.kms.KmsDefinition;
import io.strimzi.kafka.topicenc.kms.test.TestKms;
import io.strimzi.kafka.topicenc.policy.InMemoryPolicyRepository;
import io.strimzi.kafka.topicenc.policy.TopicPolicy;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.Errors;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class TopicEncryptionFilter implements ProduceRequestFilter, FetchResponseFilter, MetadataResponseFilter {

    private final Map<Uuid, String> topicUuidToName = new HashMap<>();

    private final EncryptionModule module = new EncryptionModule(new InMemoryPolicyRepository(List.of(new TopicPolicy().setTopic(TopicPolicy.ALL_TOPICS).setKms(new TestKms(new KmsDefinition())))));

    @Override
    public void onFetchResponse(short apiVersion, ResponseHeaderData header, FetchResponseData response, KrpcFilterContext context) {
        boolean allTopicsResolvableToName = response.responses().stream().allMatch(this::isResolvable);
        if (allTopicsResolvableToName) {
            decryptFetchResponse(header, response, context);
        } else {
            resolveTopicsThenDecrypt(apiVersion, header, response, context);
        }
    }

    private void resolveTopicsThenDecrypt(short apiVersion, ResponseHeaderData header, FetchResponseData response, KrpcFilterContext context) {
        Stream<Uuid> topicIdsToResolve = response.responses().stream().filter(Predicate.not(this::isResolvable)).map(FetchResponseData.FetchableTopicResponse::topicId);
        MetadataRequestData request = new MetadataRequestData();
        topicIdsToResolve.forEach(uuid -> {
            MetadataRequestData.MetadataRequestTopic e = new MetadataRequestData.MetadataRequestTopic();
            e.setTopicId(uuid);
            request.topics().add(e);
        });
        CompletionStage<MetadataResponseData> stage = context.sendRequest(ApiMessageType.METADATA.highestSupportedVersion(), request);
        stage.thenAccept(this::cacheTopicIdToName).thenAccept(unused -> {
            onFetchResponse(apiVersion, header, response, context);
        });
    }

    private void decryptFetchResponse(ResponseHeaderData header, FetchResponseData response, KrpcFilterContext context) {
        for (FetchResponseData.FetchableTopicResponse fetchResponse : response.responses()) {
            Uuid originalUuid = fetchResponse.topicId();
            String originalName = fetchResponse.topic();
            if (originalName == null || originalName.equals("")) {
                fetchResponse.setTopic(topicUuidToName.get(originalUuid));
                fetchResponse.setTopicId(null);
            }
            try {
                module.decrypt(fetchResponse);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            fetchResponse.setTopic(originalName);
            fetchResponse.setTopicId(originalUuid);
        }
        context.forwardResponse(header, response);
    }


    private boolean isResolvable(FetchResponseData.FetchableTopicResponse fetchableTopicResponse) {
        return (fetchableTopicResponse.topic() != null && !fetchableTopicResponse.topic().equals("")) || topicUuidToName.containsKey(fetchableTopicResponse.topicId());
    }

    @Override
    public void onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, KrpcFilterContext context) {
        try {
            for (ProduceRequestData.TopicProduceData topicDatum : request.topicData()) {
                try {
                    module.encrypt(topicDatum);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
            context.forwardRequest(header, request);
        } catch (Exception e) {
            sendErrorProduceResponse(request, context);
        }
    }

    private static void sendErrorProduceResponse(ProduceRequestData request, KrpcFilterContext context) {
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
        context.forwardResponse(response);
    }

    @Override
    public void onMetadataResponse(short apiVersion, ResponseHeaderData header, MetadataResponseData response, KrpcFilterContext context) {
        cacheTopicIdToName(response);
        context.forwardResponse(header, response);
    }

    private void cacheTopicIdToName(MetadataResponseData response) {
        response.topics().forEach(topic -> {
            topicUuidToName.put(topic.topicId(), topic.name());
        });
    }
}
