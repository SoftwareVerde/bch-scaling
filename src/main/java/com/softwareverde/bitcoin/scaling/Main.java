package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.CoreInflater;
import com.softwareverde.bitcoin.address.Address;
import com.softwareverde.bitcoin.address.AddressInflater;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockDeflater;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.block.validator.difficulty.DifficultyCalculator;
import com.softwareverde.bitcoin.context.DifficultyCalculatorContext;
import com.softwareverde.bitcoin.inflater.MasterInflater;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.rpc.BitcoinMiningRpcConnectorFactory;
import com.softwareverde.bitcoin.rpc.BitcoinNodeRpcAddress;
import com.softwareverde.bitcoin.rpc.BitcoinVerdeRpcConnector;
import com.softwareverde.bitcoin.rpc.BlockTemplate;
import com.softwareverde.bitcoin.rpc.MutableBlockTemplate;
import com.softwareverde.bitcoin.scaling.generate.rpc.PrivateTestNetBitcoinMiningRpcConnector;
import com.softwareverde.bitcoin.secp256k1.privatekey.PrivateKeyDeflater;
import com.softwareverde.bitcoin.server.main.BitcoinConstants;
import com.softwareverde.bitcoin.stratum.BitcoinCoreStratumServer;
import com.softwareverde.bitcoin.stratum.StratumServer;
import com.softwareverde.bitcoin.stratum.callback.BlockFoundCallback;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.concurrent.threadpool.CachedThreadPool;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.logging.LineNumberAnnotatedLog;
import com.softwareverde.logging.LogLevel;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.Container;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.Util;

import java.io.File;
import java.util.HashMap;

public class Main {
    public static void main(final String[] commandLineArguments) {
        Logger.setLog(LineNumberAnnotatedLog.getInstance());
        Logger.setLogLevel(LogLevel.DEBUG);
        Logger.setLogLevel("com.softwareverde.util", LogLevel.ERROR);
        Logger.setLogLevel("com.softwareverde.network", LogLevel.INFO);
        Logger.setLogLevel("com.softwareverde.async.lock", LogLevel.WARN);
        Logger.setLogLevel("com.softwareverde.bitcoin.stratum.BitcoinCoreStratumServer", LogLevel.WARN);

        final Main main = new Main();
        main.run();
    }

    protected final MutableList<Block> _blocks;

    protected void _initBlocks(final File blocksBaseDirectory) {
        final Sha256Hash genesisBlockHash = Sha256Hash.fromHexString(BitcoinConstants.TestNet4.genesisBlockHash);

        Sha256Hash lastLoadedBlockHash = null;
        for (int i = 0; i < 3; ++i) {
            final File blocksDirectory = new File(blocksBaseDirectory, String.valueOf(i));
            if (! blocksDirectory.isDirectory()) { break; }

            final File[] blocksDirectoryList = blocksDirectory.listFiles();
            if (blocksDirectoryList == null) { break; }

            final HashMap<Sha256Hash, Block> previousBlockHashMap = new HashMap<>();

            final BlockInflater blockInflater = new BlockInflater();
            for (final File blockFile : blocksDirectoryList) {
                final ByteArray blockData = ByteArray.wrap(IoUtil.getFileContents(blockFile));
                final Block block = blockInflater.fromBytes(blockData);
                if (block == null) { continue; }

                // If the genesis block hasn't been loaded, add it to the list when it appears.
                if (lastLoadedBlockHash == null) {
                    final Sha256Hash blockHash = block.getHash();
                    if (Util.areEqual(blockHash, genesisBlockHash)) {
                        _blocks.add(block);
                        lastLoadedBlockHash = genesisBlockHash;

                        _sendBlock(block);
                    }
                }

                final Sha256Hash previousBlockHash = block.getPreviousBlockHash();
                previousBlockHashMap.put(previousBlockHash, block);
            }

            while (true) {
                final Block block = previousBlockHashMap.get(lastLoadedBlockHash);
                if (block == null) { break; }

                _blocks.add(block);
                lastLoadedBlockHash = block.getHash();

                _sendBlock(block);
            }
        }

        Logger.debug(_blocks.getCount() + " blocks loaded.");
        Logger.debug("HeadBlock=" + _blocks.get(_blocks.getCount() - 1).getHash());
    }

    public Main() {
        _blocks = new MutableList<>(10000);
    }

    protected BlockTemplate _createBlockTemplate(final Sha256Hash previousBlockHash, final Long blockHeight, final List<Transaction> transactions) {
        final DifficultyCalculatorContext difficultyCalculatorContext = new PrivateTestNetDifficultyCalculatorContext(_blocks);
        final DifficultyCalculator difficultyCalculator = new DifficultyCalculator(difficultyCalculatorContext);

        final Difficulty difficulty = difficultyCalculator.calculateRequiredDifficulty(blockHeight);

        final Long coinbaseReward = BlockHeader.calculateBlockReward(blockHeight);

        final MutableBlockTemplate blockTemplate = new MutableBlockTemplate();
        blockTemplate.setBlockVersion(Block.VERSION);
        blockTemplate.setDifficulty(difficulty);
        blockTemplate.setPreviousBlockHash(previousBlockHash);
        blockTemplate.setBlockHeight(blockHeight);
        blockTemplate.setCoinbaseAmount(coinbaseReward);

        for (final Transaction transaction : transactions) {
            blockTemplate.addTransaction(transaction, 0L, 0);
        }

        return blockTemplate;
    }

    protected void _sendBlock(final Block block) {
        final BitcoinNodeRpcAddress bitcoinNodeRpcAddress = new BitcoinNodeRpcAddress("localhost", 8334);
        try (final BitcoinVerdeRpcConnector bitcoinVerdeRpcConnector = new BitcoinVerdeRpcConnector(bitcoinNodeRpcAddress, null)) {
            bitcoinVerdeRpcConnector.submitBlock(block);
        }
    }

    public void run() {
        final File blocksBaseDirectory = new File("data/blocks");
        _initBlocks(blocksBaseDirectory);

        final Address address;
        final PrivateKey privateKey = PrivateKey.createNewKey();
        {
            final PrivateKeyDeflater privateKeyDeflater = new PrivateKeyDeflater();
            final String privateKeyWif = privateKeyDeflater.toWalletImportFormat(privateKey, true);
            System.out.println("Private Key: " + privateKeyWif);

            final AddressInflater addressInflater = new AddressInflater();
            address = addressInflater.fromPrivateKey(privateKey, true);
        }

        final Container<BlockTemplate> blockTemplateContainer;
        {
            final int blocksCount = _blocks.getCount();
            final Block previousBlock = _blocks.get(blocksCount - 1);
            final Sha256Hash previousBlockHash = previousBlock.getHash();

            final Long nextBlockHeight = (long) blocksCount;
            final BlockTemplate blockTemplate = _createBlockTemplate(previousBlockHash, nextBlockHeight, new MutableList<>(0));
            blockTemplateContainer = new Container<>(blockTemplate);
        }

        final BitcoinMiningRpcConnectorFactory rpcConnectorFactory = new BitcoinMiningRpcConnectorFactory() {
            @Override
            public BitcoinMiningRpcConnector newBitcoinMiningRpcConnector() {
                return new PrivateTestNetBitcoinMiningRpcConnector(blockTemplateContainer.value);
            }
        };

        final BlockDeflater blockDeflater = new BlockDeflater();
        final MasterInflater masterInflater = new CoreInflater();
        final CachedThreadPool threadPool = new CachedThreadPool(32, 10000L);
        final StratumServer stratumServer = new BitcoinCoreStratumServer(rpcConnectorFactory, 3333, threadPool, masterInflater);

        final File scenarioDirectory = new File(blocksBaseDirectory, "default");
        if (! scenarioDirectory.exists()) { scenarioDirectory.mkdir(); }

        final Container<Long> scenarioBlockCount = new Container<>(0L);
        stratumServer.setBlockFoundCallback(new BlockFoundCallback() {
            @Override
            public synchronized void run(final Block block, final String workerName) {
                final Sha256Hash blockHash = block.getHash();

                {
                    final Block previousBlock = _blocks.get(_blocks.getCount() - 1);
                    final Sha256Hash expectedPreviousBlockHash = previousBlock.getHash();
                    final Sha256Hash previousBlockHash = block.getPreviousBlockHash();

                    if (! Util.areEqual(expectedPreviousBlockHash, previousBlockHash)) {
                        Logger.debug("Rejected: " + blockHash + " " + expectedPreviousBlockHash + " != " + previousBlockHash);
                        return;
                    }
                }

                final ByteArray blockBytes = blockDeflater.toBytes(block);

                final int subDirectoryIndex = (int) (scenarioBlockCount.value / 2016L);
                final File subDirectory = new File(scenarioDirectory, String.valueOf(subDirectoryIndex));
                if (! subDirectory.exists()) { subDirectory.mkdir(); }

                final File file = new File(subDirectory, blockHash.toString());
                IoUtil.putFileContents(file, blockBytes);

                scenarioBlockCount.value += 1L;

                Logger.info(_blocks.getCount() + "-" + scenarioBlockCount.value + " " + file.getPath());

                _blocks.add(block);

                final long nextBlockHeight = _blocks.getCount();
                blockTemplateContainer.value = _createBlockTemplate(blockHash, nextBlockHeight, new MutableList<>(0));

                _sendBlock(block);
            }
        });
        stratumServer.setCoinbaseAddress(address);

        threadPool.start();
        stratumServer.start();

        while (true) {
            try {
                Thread.sleep(10000);
            }
            catch (final Exception exception) { break; }
        }

        stratumServer.stop();
        threadPool.stop();
    }
}