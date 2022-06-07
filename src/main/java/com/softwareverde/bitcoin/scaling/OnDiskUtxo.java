package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.TransactionInflater;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;

import java.io.File;

public class OnDiskUtxo extends TestUtxo {
    public static TestUtxo fromTransaction(final Transaction transaction, final Integer outputIndex, final Long blockHeight, final Sha256Hash blockHash, final Long blockOffset, final File blocksDirectory) {
        final Integer transactionByteCount = transaction.getByteCount();
        if (transactionByteCount < 512) { return new TestUtxo(transaction, outputIndex, blockHeight); }

        final List<TransactionOutput> transactionOutputs = transaction.getTransactionOutputs();
        final TransactionOutput transactionOutput = transactionOutputs.get(outputIndex);
        final Long outputAmount = transactionOutput.getAmount();

        final Sha256Hash transactionHash = transaction.getHash();

        return  new OnDiskUtxo(transactionHash, transactionByteCount, outputAmount, outputIndex, blockHash, blockHeight, blockOffset, blocksDirectory);
    }

    protected final Sha256Hash _transactionHash;
    protected final Long _blockOffset;
    protected final Integer _transactionByteCount;
    protected final Long _amount;

    protected final Sha256Hash _blockHash;
    protected final File _blocksDirectory;

    public OnDiskUtxo(final Sha256Hash transactionHash, final Integer transactionByteCount, final Long outputAmount, final Integer outputIndex, final Sha256Hash blockHash, final Long blockHeight, final Long blockOffset, final File blocksDirectory) {
        super(null, outputIndex, blockHeight); // Avoid keeping the transaction in memory...

        _transactionHash = transactionHash;
        _transactionByteCount = transactionByteCount;
        _blockOffset = blockOffset;

        _blockHash = blockHash;
        _blocksDirectory = blocksDirectory;

        _amount = outputAmount;
    }

    @Override
    public Sha256Hash getTransactionHash() {
        return _transactionHash;
    }

    @Override
    public Long getAmount() {
        return _amount;
    }

    @Override
    public Transaction getTransaction() {
        final ByteArray transactionBytes = DiskUtil.readFromBlock(_blockHash, _blockOffset, _transactionByteCount, _blocksDirectory);
        final TransactionInflater transactionInflater = new TransactionInflater();
        return transactionInflater.fromBytes(transactionBytes);
    }
}