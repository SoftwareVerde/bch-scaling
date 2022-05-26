package com.softwareverde.bitcoin.wallet;

import com.softwareverde.bitcoin.transaction.output.ImmutableUnspentTransactionOutput;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.constable.Const;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;

public class WalletUtxo extends ImmutableUnspentTransactionOutput implements SpendableTransactionOutput, Const {
    protected final Sha256Hash _transactionHash;

    public WalletUtxo(final TransactionOutput transactionOutput, final Sha256Hash transactionHash, final Long blockHeight, final Boolean isCoinbase) {
        super(transactionOutput, blockHeight, isCoinbase);
        _transactionHash = transactionHash;
    }

    @Override
    public Sha256Hash getTransactionHash() {
        return _transactionHash;
    }

    @Override
    public WalletUtxo asConst() {
        return this;
    }
}
