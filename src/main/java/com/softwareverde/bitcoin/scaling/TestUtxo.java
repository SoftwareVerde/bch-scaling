package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.constable.list.List;
import com.softwareverde.util.Util;

public class TestUtxo {
    protected final Transaction _transaction;
    protected final Integer _outputIndex;
    protected final Long _blockHeight;

    public TestUtxo(final Transaction transaction, final Integer outputIndex, final Long blockHeight) {
        _outputIndex = outputIndex;
        _blockHeight = blockHeight;
        _transaction = transaction;
    }

    public Long getAmount() {
        final List<TransactionOutput> transactionOutputs = _transaction.getTransactionOutputs();
        final TransactionOutput transactionOutput = transactionOutputs.get(_outputIndex);
        return transactionOutput.getAmount();
    }

    public Transaction getTransaction() {
        return _transaction;
    }

    public Integer getOutputIndex() {
        return _outputIndex;
    }

    public Long getBlockHeight() {
        return _blockHeight;
    }

    @Override
    public boolean equals(final Object object) {
        if (! (object instanceof TestUtxo)) { return false; }

        final TestUtxo testUtxo = (TestUtxo) object;
        if (! Util.areEqual(_transaction.getHash(), testUtxo._transaction.getHash())) { return false; }
        if (! Util.areEqual(_outputIndex, testUtxo._outputIndex)) { return false; }
        if (! Util.areEqual(this.getAmount(), testUtxo.getAmount())) { return false; }
        if (! Util.areEqual(_blockHeight, testUtxo._blockHeight)) { return false; }

        return true;
    }

    @Override
    public int hashCode() {
        return (_transaction.getHash().hashCode() + _outputIndex.hashCode() + this.getAmount().hashCode() + _blockHeight.hashCode());
    }
}
