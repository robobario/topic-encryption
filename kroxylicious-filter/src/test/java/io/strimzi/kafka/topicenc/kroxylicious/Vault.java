package io.strimzi.kafka.topicenc.kroxylicious;

import org.testcontainers.vault.VaultContainer;

import java.io.Closeable;
import java.util.UUID;
import java.util.function.Consumer;

public class Vault {
    private static final String LATEST_VAULT_IMAGE = "hashicorp/vault:1.13";

    public record VaultTester(VaultContainer<?> vault, String token) implements Closeable {

        @Override
        public void close() {
            vault.close();
        }
    }

    public static VaultTester startVaultContainer(Consumer<VaultContainer<?>> withConfiguration) {
        String vaultToken = UUID.randomUUID().toString();
        VaultContainer<?> vaultContainer = new VaultContainer<>(LATEST_VAULT_IMAGE)
                .withVaultToken(vaultToken)
                .withInitCommand("secrets enable transit", "write -f transit/keys/my-key");
        withConfiguration.accept(vaultContainer);
        vaultContainer.start();
        return new VaultTester(vaultContainer, vaultToken);
    }
}
