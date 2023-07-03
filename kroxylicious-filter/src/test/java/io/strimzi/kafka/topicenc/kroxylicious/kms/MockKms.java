package io.strimzi.kafka.topicenc.kroxylicious.kms;

import io.strimzi.kafka.topicenc.common.EncUtils;
import io.strimzi.kafka.topicenc.kms.KeyMgtSystem;

import javax.crypto.SecretKey;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

public class MockKms implements KeyMgtSystem {
    private final Map<String, SecretKey> secrets;

    // hacky but with the current API KMS only has a few string values available to it, we are abusing the credential field
    // to pass in a map from key reference to key.
    public MockKms(String keys) {
        secrets = Arrays.stream(keys.split(",")).map(s -> s.split(":"))
                .collect(Collectors.toMap(strings -> strings[0], strings -> EncUtils.base64Decode(strings[1])));
    }

    @Override
    public SecretKey getKey(String keyReference) {
        if(!secrets.containsKey(keyReference)){
            throw new IllegalArgumentException("expect all key references to have a secret");
        }
        return secrets.get(keyReference);
    }

    public static String encodeCredentialString(Map<String, SecretKey> keyReferenceToSecret){
        return keyReferenceToSecret.entrySet().stream().map(stringSecretKeyEntry -> stringSecretKeyEntry.getKey() + ":" + EncUtils.base64Encode(stringSecretKeyEntry.getValue())).collect(Collectors.joining(","));
    }
}
