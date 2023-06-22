package io.strimzi.kafka.topicenc.kroxylicious;

import io.strimzi.kafka.topicenc.common.EncUtils;
import io.strimzi.kafka.topicenc.enc.AesGcmEncrypter;
import io.strimzi.kafka.topicenc.enc.EncData;
import io.strimzi.kafka.topicenc.ser.AesGcmV1SerDer;

import javax.crypto.SecretKey;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TestCrypter {
    private static final AesGcmV1SerDer aesGcmV1SerDer = new AesGcmV1SerDer();

    public static SecretKey randomAesKey() {
        try {
            return EncUtils.generateAesKey(256);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] toEncryptedRecordValue(byte[] unencryptedBytes, SecretKey key) {
        try {
            AesGcmEncrypter crypterForKey = new AesGcmEncrypter(key);
            return aesGcmV1SerDer.serialize(crypterForKey.encrypt(unencryptedBytes));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] toDecryptedRecordValue(byte[] encryptedRecordValue, SecretKey key) {
        try {
            EncData encData = aesGcmV1SerDer.deserialize(encryptedRecordValue);
            AesGcmEncrypter crypterForKey = new AesGcmEncrypter(key);
            return crypterForKey.decrypt(encData);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, SecretKey> uniqueKeyPerKeyReference(Set<String> keyReferences) {
        return keyReferences.stream().collect(Collectors.toMap(n -> n, n -> randomAesKey()));
    }
}
