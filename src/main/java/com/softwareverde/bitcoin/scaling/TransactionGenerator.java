package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.constable.list.List;

public interface TransactionGenerator {
    List<Transaction> getTransactions(Long blockHeight, List<BlockHeader> newBlockHeaders);
}