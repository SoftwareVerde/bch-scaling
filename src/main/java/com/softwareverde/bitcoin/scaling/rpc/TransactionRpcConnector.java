package com.softwareverde.bitcoin.scaling.rpc;

import com.softwareverde.bitcoin.transaction.Transaction;

public interface TransactionRpcConnector extends AutoCloseable {
    void submitTransaction(Transaction transaction);
}
