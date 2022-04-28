package com.softwareverde.bitcoin.scaling;

import com.softwareverde.cryptography.secp256k1.key.PrivateKey;

public interface PrivateKeyGenerator {
    PrivateKey getCoinbasePrivateKey(Long blockHeight);
}
