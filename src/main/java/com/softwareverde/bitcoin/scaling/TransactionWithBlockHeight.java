package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.transaction.Transaction;

public class TransactionWithBlockHeight {
    public final Transaction transaction;
    public final Long blockHeight;

    public TransactionWithBlockHeight(final Transaction transaction, final Long blockHeight) {
        this.transaction = transaction;
        this.blockHeight = blockHeight;
    }
}
