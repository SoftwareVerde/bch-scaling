package com.softwareverde.bitcoin.wallet;

import com.softwareverde.bitcoin.transaction.output.UnspentTransactionOutput;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;

public interface SpendableTransactionOutput extends UnspentTransactionOutput {
    Sha256Hash getTransactionHash();
}
