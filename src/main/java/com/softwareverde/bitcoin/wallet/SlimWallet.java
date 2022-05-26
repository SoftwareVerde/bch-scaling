package com.softwareverde.bitcoin.wallet;

import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.bip.CoreUpgradeSchedule;
import com.softwareverde.bitcoin.bip.UpgradeSchedule;
import com.softwareverde.bitcoin.transaction.MutableTransaction;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.input.MutableTransactionInput;
import com.softwareverde.bitcoin.transaction.input.TransactionInput;
import com.softwareverde.bitcoin.transaction.locktime.LockTime;
import com.softwareverde.bitcoin.transaction.locktime.SequenceNumber;
import com.softwareverde.bitcoin.transaction.output.MutableTransactionOutput;
import com.softwareverde.bitcoin.transaction.output.TransactionOutput;
import com.softwareverde.bitcoin.transaction.script.ScriptBuilder;
import com.softwareverde.bitcoin.transaction.script.ScriptPatternMatcher;
import com.softwareverde.bitcoin.transaction.script.ScriptType;
import com.softwareverde.bitcoin.transaction.script.locking.LockingScript;
import com.softwareverde.bitcoin.transaction.script.signature.hashtype.HashType;
import com.softwareverde.bitcoin.transaction.script.signature.hashtype.Mode;
import com.softwareverde.bitcoin.transaction.script.unlocking.UnlockingScript;
import com.softwareverde.bitcoin.transaction.signer.SignatureContext;
import com.softwareverde.bitcoin.transaction.signer.TransactionSigner;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.ripemd160.Ripemd160Hash;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.cryptography.secp256k1.key.PublicKey;

import java.util.HashMap;

public class SlimWallet {
    public static MutableList<SpendableTransactionOutput> getTransactionOutputs(final Transaction transaction, final Long blockHeight, final Boolean isCoinbase) {
        final List<TransactionOutput> transactionOutputs = transaction.getTransactionOutputs();
        final int outputCount = transactionOutputs.getCount();

        final Sha256Hash transactionHash = transaction.getHash();

        final MutableList<SpendableTransactionOutput> spendableTransactionOutputs = new MutableList<>(outputCount);
        for (final TransactionOutput transactionOutput : transactionOutputs) {
            final WalletUtxo walletUtxo = new WalletUtxo(transactionOutput, transactionHash, blockHeight, isCoinbase);
            spendableTransactionOutputs.add(walletUtxo);
        }

        return spendableTransactionOutputs;
    }

    protected final UpgradeSchedule _upgradeSchedule;
    protected final HashMap<PublicKey, PrivateKey> _privateKeys = new HashMap<>();
    protected final HashMap<Address, PublicKey> _publicKeys = new HashMap<>();
    protected final MutableList<SpendableTransactionOutput> _unspentTransactionOutputs = new MutableList<>();

    protected Long _feePerByte = 1L;

    protected Boolean _hasPrivateKey(final SpendableTransactionOutput spendableTransactionOutput) {
        final ScriptPatternMatcher scriptPatternMatcher = new ScriptPatternMatcher();

        final LockingScript lockingScript = spendableTransactionOutput.getLockingScript();
        final ScriptType scriptType = scriptPatternMatcher.getScriptType(lockingScript);
        final Address address = scriptPatternMatcher.extractAddress(scriptType, lockingScript);

        final PublicKey publicKey = _publicKeys.get(address);
        if (publicKey == null) { return false; }

        return _privateKeys.containsKey(publicKey);
    }

    public SlimWallet() {
        _upgradeSchedule = new CoreUpgradeSchedule();
    }

    public SlimWallet(final UpgradeSchedule upgradeSchedule) {
        _upgradeSchedule = upgradeSchedule;
    }

    public Transaction createTransaction(final List<PaymentAmount> paymentAmounts, final Address changeAddress) {
        if (paymentAmounts.isEmpty()) { return null; }

        final MutableTransaction transaction = new MutableTransaction();
        transaction.setVersion(Transaction.VERSION);
        transaction.setLockTime(LockTime.MIN_TIMESTAMP);

        long totalAmountAvailable = 0L;
        for (final SpendableTransactionOutput unspentTransactionOutput : _unspentTransactionOutputs) {
            final MutableTransactionInput transactionInput = new MutableTransactionInput();
            transactionInput.setSequenceNumber(SequenceNumber.MAX_SEQUENCE_NUMBER);
            transactionInput.setPreviousOutputTransactionHash(unspentTransactionOutput.getTransactionHash());
            transactionInput.setPreviousOutputIndex(unspentTransactionOutput.getIndex());
            transactionInput.setUnlockingScript(UnlockingScript.EMPTY_SCRIPT);
            transaction.addTransactionInput(transactionInput);

            totalAmountAvailable += unspentTransactionOutput.getAmount();
        }

        int transactionOutputIndex = 0;
        long totalAmountSpent = 0L;
        for (final PaymentAmount paymentAmount : paymentAmounts) {
            final MutableTransactionOutput transactionOutput = new MutableTransactionOutput();
            transactionOutput.setIndex(transactionOutputIndex);
            transactionOutput.setAmount(paymentAmount.amount);
            final LockingScript lockingScript;
            {
                if (paymentAmount.address.getType() == Address.Type.P2SH) {
                    lockingScript = ScriptBuilder.payToScriptHash(Ripemd160Hash.wrap(paymentAmount.address.getBytes()));
                }
                else {
                    lockingScript = ScriptBuilder.payToAddress(paymentAmount.address);
                }
            }
            transactionOutput.setLockingScript(lockingScript);
            transaction.addTransactionOutput(transactionOutput);

            transactionOutputIndex += 1;

            totalAmountSpent += paymentAmount.amount;
        }

        final long minChange = 546L;
        final long maxChange = (totalAmountAvailable - totalAmountSpent);
        if (maxChange < 0L) { return null; }

        final int preChangeByteCount = transaction.getByteCount();
        final long preChangeMinFee = (_feePerByte * preChangeByteCount);
        if (maxChange < preChangeMinFee) { return null; }

        // Handle change address, if applicable.
        if ( (changeAddress != null) && (maxChange > minChange) ) {
            final MutableTransactionOutput transactionOutput = new MutableTransactionOutput();
            transactionOutput.setIndex(transactionOutputIndex);
            transactionOutput.setAmount(0L);
            final LockingScript lockingScript;
            {
                if (changeAddress.getType() == Address.Type.P2SH) {
                    lockingScript = ScriptBuilder.payToScriptHash(Ripemd160Hash.wrap(changeAddress.getBytes()));
                }
                else {
                    lockingScript = ScriptBuilder.payToAddress(changeAddress);
                }
            }
            transactionOutput.setLockingScript(lockingScript);
            transaction.addTransactionOutput(transactionOutput);

            final int postChangeByteCount = transaction.getByteCount();
            final long feeWithChangeOutput = (_feePerByte * postChangeByteCount);
            final long changeWithFee = (maxChange - feeWithChangeOutput);
            if (changeWithFee >= minChange) {
                transactionOutput.setAmount(changeWithFee);
                transaction.setTransactionOutput(transactionOutputIndex, transactionOutput); // Replace the existing output since it was copied during add.
            }
        }

        final TransactionSigner transactionSigner = new TransactionSigner();
        final ScriptPatternMatcher scriptPatternMatcher = new ScriptPatternMatcher();

        final Transaction signedTransaction;
        {
            Transaction transactionBeingSigned = transaction;
            final List<TransactionInput> transactionInputs = transaction.getTransactionInputs();
            for (int inputIndex = 0; inputIndex < transactionInputs.getCount(); ++inputIndex) {
                final SpendableTransactionOutput transactionOutputBeingSpent = _unspentTransactionOutputs.get(inputIndex);

                final PrivateKey privateKey;
                final Boolean useCompressedPublicKey;
                {
                    final LockingScript lockingScript = transactionOutputBeingSpent.getLockingScript();
                    final ScriptType scriptType = scriptPatternMatcher.getScriptType(lockingScript);
                    final Address addressBeingSpent = scriptPatternMatcher.extractAddress(scriptType, lockingScript);
                    final PublicKey publicKey = _publicKeys.get(addressBeingSpent);
                    privateKey = _privateKeys.get(publicKey);
                    useCompressedPublicKey = publicKey.isCompressed();
                }

                final HashType hashType = new HashType(Mode.SIGNATURE_HASH_ALL, true, true);
                final SignatureContext signatureContext = new SignatureContext(transactionBeingSigned, hashType, _upgradeSchedule);
                signatureContext.setInputIndexBeingSigned(inputIndex);
                signatureContext.setShouldSignInputScript(inputIndex, true, transactionOutputBeingSpent);
                transactionBeingSigned = transactionSigner.signTransaction(signatureContext, privateKey, useCompressedPublicKey);
            }

            signedTransaction = transactionBeingSigned;
        }

        return signedTransaction;
    }

    public void addPrivateKey(final PrivateKey privateKey) {
        final PrivateKey constPrivateKey = privateKey.asConst();
        final PublicKey publicKey = constPrivateKey.getPublicKey();

        final PublicKey compressedPublicKey = publicKey.compress();
        final PublicKey decompressedPublicKey = publicKey.decompress();

        final AddressInflater addressInflater = new AddressInflater();
        final Address decompressedAddress = addressInflater.fromPrivateKey(constPrivateKey, false);
        final Address compressedAddress = addressInflater.fromPrivateKey(constPrivateKey, true);

        _privateKeys.put(compressedPublicKey.asConst(), constPrivateKey);
        _privateKeys.put(decompressedPublicKey.asConst(), constPrivateKey);

        _publicKeys.put(compressedAddress, compressedPublicKey);
        _publicKeys.put(decompressedAddress, decompressedPublicKey);
    }

    public void addUnspentTransactionOutput(final SpendableTransactionOutput unspentTransactionOutput) {
        final boolean hasPrivateKey = _hasPrivateKey(unspentTransactionOutput);
        if (! hasPrivateKey) { return; }

        _unspentTransactionOutputs.add(unspentTransactionOutput);
    }

    public List<SpendableTransactionOutput> getUnspentTransactionOutputs() {
        return _unspentTransactionOutputs.asConst();
    }

    public Long getBalance() {
        long value = 0L;
        for (final SpendableTransactionOutput spendableTransactionOutput : _unspentTransactionOutputs) {
            value += spendableTransactionOutput.getAmount();
        }
        return value;
    }

    public void clear() {
        _privateKeys.clear();
        _publicKeys.clear();
        _unspentTransactionOutputs.clear();
    }
}
