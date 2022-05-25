package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.constable.list.List;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.util.Util;

public class TestUtxo {
    protected final Integer _outputIndex;
    protected final Long _blockHeight;
    protected final Transaction _transaction;

    protected Integer _cachedHashCode;
    protected int _getHashCode() {
        if (_cachedHashCode == null) {
            _cachedHashCode = (this.getTransactionHash().hashCode() + this.getOutputIndex().hashCode() + this.getAmount().hashCode() + this.getBlockHeight().hashCode());
        }
        return _cachedHashCode;
    }

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

    public Sha256Hash getTransactionHash() {
        return _transaction.getHash();
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

        final int hashCode = this.hashCode();
        final int testUtxoHashCode = testUtxo.hashCode();
        if (hashCode != testUtxoHashCode) { return false; }

        if (! Util.areEqual(this.getTransactionHash(), testUtxo.getTransactionHash())) { return false; }
        if (! Util.areEqual(this.getOutputIndex(), testUtxo.getOutputIndex())) { return false; }
        if (! Util.areEqual(this.getBlockHeight(), testUtxo.getBlockHeight())) { return false; }

        return true;
    }

    @Override
    public int hashCode() {
        return _getHashCode();
    }
}
