package com.softwareverde.bitcoin.scaling.generate;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.scaling.BlockSender;
import com.softwareverde.bitcoin.scaling.DiskUtil;
import com.softwareverde.bitcoin.scaling.Main;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.transaction.input.TransactionInput;
import com.softwareverde.bitcoin.wallet.SlimWallet;
import com.softwareverde.bitcoin.wallet.SpendableTransactionOutput;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.json.Json;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.StringUtil;

import java.io.File;
import java.util.HashMap;

public class TestBlockMiner {
    protected BlockSender _blockSender;
    protected File _manifestFile;
    protected File _scenarioDirectory;
    protected boolean _shouldPerformProofOfWork;

    public TestBlockMiner(final BlockSender blockSender, final File manifestFile, final File scenarioDirectory, final boolean shouldPerformProofOfWork) {
        _blockSender = blockSender;
        _manifestFile = manifestFile;
        _scenarioDirectory = scenarioDirectory;
        _shouldPerformProofOfWork = shouldPerformProofOfWork;
    }

    public void mine(final MutableList<BlockHeader> blockHeaders, final Json blocksManifestJson, int manifestBlockCount, final Json reorgBlocksManifestJson) {
        final int coinbaseMaturityBlockCount = 100;

        final PrivateKeyGenerator privateKeyGenerator = new PrivateKeyGenerator() {
            @Override
            public PrivateKey getCoinbasePrivateKey(final Long blockHeight) {
                return Main.derivePrivateKey(blockHeight, 50L * Transaction.SATOSHIS_PER_BITCOIN);
            }
        };

        final Json newManifestJson = new Json(true);

        // Copy the existing blocks to the new manifest file...
        if (blocksManifestJson != null) {
            for (int i = 0; i < manifestBlockCount; ++i) {
                final String blockHashString = blocksManifestJson.getString(i);
                newManifestJson.add(blockHashString);
            }
        }
        long runningBlockHeight = (blockHeaders.getCount() - 1L);

        final BlockGenerator blockGenerator = new BlockGenerator(_blockSender, privateKeyGenerator, _scenarioDirectory);
        blockGenerator.setProofOfWorkEnabled(!_shouldPerformProofOfWork);

        final Long firstSpendableCoinbaseBlockHeight = (long) blockHeaders.getCount();
        Logger.info("Generating spendable coinbase blocks: " + runningBlockHeight);
        // final long firstScenarioBlockHeight = blockHeaders.getCount();
        {
            final int targetNewBlockCount = coinbaseMaturityBlockCount;
            final List<BlockHeader> scenarioBlocks = blockGenerator.generateBlocks(blocksManifestJson, runningBlockHeight, targetNewBlockCount, blockHeaders, null);
            blockHeaders.addAll(scenarioBlocks);
            _updateManifest(newManifestJson, scenarioBlocks);
            runningBlockHeight += scenarioBlocks.getCount();
        }

        Logger.info("Generating fan-out blocks: " + runningBlockHeight);
        final Long firstFanOutBlockHeight = (long) blockHeaders.getCount();
        {
            final int targetNewBlockCount = 10;
            final TransactionGenerator transactionGenerator = new FanOutBlockTransactionGenerator(privateKeyGenerator, _scenarioDirectory, coinbaseMaturityBlockCount);
            final List<BlockHeader> fanOutBlocks = blockGenerator.generateBlocks(blocksManifestJson, runningBlockHeight, targetNewBlockCount, blockHeaders, transactionGenerator);
            blockHeaders.addAll(fanOutBlocks);
            _updateManifest(newManifestJson, fanOutBlocks);
            runningBlockHeight += fanOutBlocks.getCount();
        }

        Logger.info("Generating quasi-steady-state blocks: " + runningBlockHeight);
        final Long firstQuasiSteadyStateBlockHeight = (long) blockHeaders.getCount();
        {
            final int targetNewBlockCount = 5;
            final TransactionGenerator transactionGenerator = new QuasiSteadyStateRound1BlockTransactionGenerator(privateKeyGenerator, _scenarioDirectory, coinbaseMaturityBlockCount, firstQuasiSteadyStateBlockHeight, firstFanOutBlockHeight);
            final List<BlockHeader> quasiSteadyStateBlocks = blockGenerator.generateBlocks(blocksManifestJson, runningBlockHeight, targetNewBlockCount, blockHeaders, transactionGenerator);
            blockHeaders.addAll(quasiSteadyStateBlocks);
            _updateManifest(newManifestJson, quasiSteadyStateBlocks);
            runningBlockHeight += quasiSteadyStateBlocks.getCount();
        }

        Logger.info("Generating fan-in blocks: " + runningBlockHeight);
        final Long firstFanInBlockHeight = (long) blockHeaders.getCount();
        {
            final int targetNewBlockCount = 2;
            final long startingBlockHeightToSpend = (firstQuasiSteadyStateBlockHeight - firstFanInBlockHeight); // spend the quasi-steady-state blocks txns in-order...
            final TransactionGenerator transactionGenerator = new FanInBlockTransactionGenerator(privateKeyGenerator, _scenarioDirectory, coinbaseMaturityBlockCount, startingBlockHeightToSpend);
            final MutableList<BlockHeader> fanInBlocks = blockGenerator.generateBlocks(blocksManifestJson, runningBlockHeight, targetNewBlockCount, blockHeaders, transactionGenerator);
            blockHeaders.addAll(fanInBlocks);
            _updateManifest(newManifestJson, fanInBlocks);
            runningBlockHeight += fanInBlocks.getCount();
        }

        Logger.info("Generating 2nd quasi-steady-state blocks: " + runningBlockHeight);
        final long firstQuasiSteadyStateBlockHeightRoundTwo = blockHeaders.getCount();
        {
            final int targetNewBlockCount = 5;
            final TransactionGenerator transactionGenerator = new QuasiSteadyStateRound2BlockTransactionGenerator(privateKeyGenerator, _scenarioDirectory, coinbaseMaturityBlockCount, firstQuasiSteadyStateBlockHeightRoundTwo, firstFanInBlockHeight, firstFanOutBlockHeight);
            final MutableList<BlockHeader> quasiSteadyStateBlocksRoundTwo = blockGenerator.generateBlocks(blocksManifestJson, runningBlockHeight, targetNewBlockCount, blockHeaders, transactionGenerator);
            blockHeaders.addAll(quasiSteadyStateBlocksRoundTwo);
            _updateManifest(newManifestJson, quasiSteadyStateBlocksRoundTwo);
            runningBlockHeight += quasiSteadyStateBlocksRoundTwo.getCount();
        }

        Logger.info("Generating steady-state blocks: " + runningBlockHeight);
        // Spends all UTXOs from block# 145, excluding coinbases.
        // Spends coinbases from 156-160 (inclusive).
        int unspentOutputCount = 0;
        final Long firstSteadyStateBlockHeight = (long) blockHeaders.getCount();
        {
            final HashMap<Sha256Hash, MutableList<SpendableTransactionOutput>> utxoMap = new HashMap<>();
            {
                long blockHeight = firstSpendableCoinbaseBlockHeight;
                while (blockHeight < blockHeaders.getCount()) {
                    // Logger.debug("Loading Block Height: " + blockHeight);
                    final BlockHeader blockHeader = blockHeaders.get((int) blockHeight);
                    final Sha256Hash blockHash = blockHeader.getHash();
                    final Block block = DiskUtil.loadBlock(blockHash, _scenarioDirectory);
                    // Logger.debug("Block Hash: " + blockHeader.getHash() + " " + (block != null ? "block" : null));

                    boolean isCoinbase = true;
                    for (final Transaction transaction : block.getTransactions()) {
                        if (isCoinbase) {
                            // final boolean isSpendableCoinbase = ((firstSpendableCoinbaseBlockHeight - blockHeight) >= 100L);
                            isCoinbase = false;

                            // if (! isSpendableCoinbase) { continue; }
                            continue;
                        }

                        final MutableList<SpendableTransactionOutput> spendableTransactionOutputs = SlimWallet.getTransactionOutputs(transaction, blockHeight, isCoinbase);
                        final Sha256Hash transactionHash = transaction.getHash();
                        utxoMap.put(transactionHash, spendableTransactionOutputs);
                        unspentOutputCount += spendableTransactionOutputs.getCount();
                    }

                    isCoinbase = true;
                    for (final Transaction transaction : block.getTransactions()) {
                        if (isCoinbase) {
                            isCoinbase = false;
                            continue;
                        }

                        for (final TransactionInput transactionInput : transaction.getTransactionInputs()) {
                            final Sha256Hash prevoutHash = transactionInput.getPreviousOutputTransactionHash();
                            if (! utxoMap.containsKey(prevoutHash)) { continue; }

                            final Integer prevoutIndex = transactionInput.getPreviousOutputIndex();
                            final MutableList<SpendableTransactionOutput> utxoSet = utxoMap.get(prevoutHash);
                            utxoSet.set(prevoutIndex, null);
                            unspentOutputCount -= 1;
                        }
                    }

                    blockHeight += 1L;
                }
            }

            final MutableList<SpendableTransactionOutput> availableUtxos = new MutableList<>(unspentOutputCount);
            for (final MutableList<SpendableTransactionOutput> list : utxoMap.values()) {
                for (final SpendableTransactionOutput spendableTransactionOutput : list) {
                    if (spendableTransactionOutput != null) {
                        availableUtxos.add(spendableTransactionOutput);
                    }
                }
            }
            Logger.debug("availableUtxos.count=" + availableUtxos.getCount());

            // Arbitrarily order/mix the UTXOs...
            availableUtxos.sort(SteadyStateBlockTransactionGenerator.testUtxoComparator);

            final int targetNewBlockCount = 10;
            final TransactionGenerator transactionGenerator = new SteadyStateBlockTransactionGenerator(privateKeyGenerator, _scenarioDirectory, coinbaseMaturityBlockCount, availableUtxos, firstSteadyStateBlockHeight);
            final MutableList<BlockHeader> steadyStateBlocks = blockGenerator.generateBlocks(blocksManifestJson, runningBlockHeight, targetNewBlockCount, blockHeaders, transactionGenerator);
            blockHeaders.addAll(steadyStateBlocks);
            _updateManifest(newManifestJson, steadyStateBlocks);
            runningBlockHeight += steadyStateBlocks.getCount();
        }

        final Json json = new Json();
        json.put("blocks", newManifestJson);
        if (reorgBlocksManifestJson != null) {
            json.put("reorgBlocks", reorgBlocksManifestJson);
        }
        IoUtil.putFileContents(_manifestFile, StringUtil.stringToBytes(json.toString()));
    }

    private void _updateManifest(Json newManifestJson, List<BlockHeader> newBlockHeaders) {
        for (final BlockHeader blockHeader : newBlockHeaders) {
            final Sha256Hash blockHash = blockHeader.getHash();
            newManifestJson.add(blockHash);
        }
    }
}