package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.ImmutableBlockHeader;
import com.softwareverde.bitcoin.rpc.BitcoinNodeRpcAddress;
import com.softwareverde.bitcoin.rpc.RpcCredentials;
import com.softwareverde.bitcoin.scaling.generate.TestBlockMiner;
import com.softwareverde.bitcoin.server.main.BitcoinConstants;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.constable.bytearray.ByteArray;
import com.softwareverde.constable.bytearray.MutableByteArray;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.cryptography.secp256k1.key.PrivateKey;
import com.softwareverde.http.HttpResponse;
import com.softwareverde.http.WebRequest;
import com.softwareverde.json.Json;
import com.softwareverde.logging.LineNumberAnnotatedLog;
import com.softwareverde.logging.LogLevel;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.IoUtil;
import com.softwareverde.util.StringUtil;
import com.softwareverde.util.Tuple;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

public class Main implements AutoCloseable {
    public static final Boolean SKIP_SEND = false;
    public static final Boolean SKIP_MINING = true;
    public static final Float PRE_RELAY_PERCENT = 0.90F;

    public static final Boolean USE_P2P_BROADCAST = true;
    public static final Boolean IS_BITCOIN_VERDE_NODE = false;

    public static void main(final String[] commandLineArguments) {
        BitcoinConstants.setBlockMaxByteCount((int) (256L * ByteUtil.Unit.Si.MEGABYTES));

        Logger.setLog(LineNumberAnnotatedLog.getInstance());
        Logger.setLogLevel(LogLevel.DEBUG);
        Logger.setLogLevel("com.softwareverde.util", LogLevel.ERROR);
        Logger.setLogLevel("com.softwareverde.network", LogLevel.INFO);
        Logger.setLogLevel("com.softwareverde.async.lock", LogLevel.WARN);
        Logger.setLogLevel("com.softwareverde.bitcoin.stratum.BitcoinCoreStratumServer", LogLevel.WARN);
        Logger.setLogLevel("com.softwareverde.bitcoin.wallet.Wallet", LogLevel.WARN);

        try (final Main main = new Main()) {
            main.run();
        }
    }

    public static Sha256Hash getBlockHash(final Long blockHeight, final List<BlockHeader> blockHeaders, final List<BlockHeader> newBlockHeaders) {
        if (blockHeight < blockHeaders.getCount()) {
            final int blockHeightInt = blockHeight.intValue();
            final BlockHeader blockHeader = blockHeaders.get(blockHeightInt);
            return blockHeader.getHash();
        }

        final int index = (int) (blockHeight - blockHeaders.getCount());
        final BlockHeader blockHeader = newBlockHeaders.get(index);
        return blockHeader.getHash();
    }

    public static PrivateKey derivePrivateKey(final Long blockHeight, final Long outputValue) {
        final MutableByteArray bytes = new MutableByteArray(PrivateKey.KEY_BYTE_COUNT);
        final byte[] blockHeightBytes = ByteUtil.longToBytes(blockHeight);
        final byte[] outputValueBytes = ByteUtil.longToBytes(outputValue);

        final int startIndex = (PrivateKey.KEY_BYTE_COUNT - blockHeightBytes.length - outputValueBytes.length);
        bytes.setBytes(startIndex, blockHeightBytes);
        bytes.setBytes(startIndex + blockHeightBytes.length, outputValueBytes);

        return PrivateKey.fromBytes(bytes);
    }

    protected final BitcoinNodeRpcAddress _rpcAddress;
    protected final RpcCredentials _rpcCredentials;
    protected BlockSender _blockSender;

    protected Tuple<MutableList<BlockHeader>, MutableList<Block>> _loadInitBlocks(final File blocksBaseDirectory) {
        final MutableList<BlockHeader> initBlockHeaders = new MutableList<>();
        final MutableList<Block> initBlocks = new MutableList<>();

        final BlockInflater blockInflater = new BlockInflater();
        final Json mainNetBlocks = Json.parse(IoUtil.getResource("/manifest.json")); // Genesis through Block #144 (inclusive)
        for (int i = 0; i < mainNetBlocks.length(); ++i) {
            final String blockHash = mainNetBlocks.getString(i);
            final File blockDirectory = new File(blocksBaseDirectory, blockHash);

            final Block block;
            final byte[] cachedBlockBytes = IoUtil.getFileContents(blockDirectory);
            if (cachedBlockBytes != null) {
                block = blockInflater.fromBytes(cachedBlockBytes);
            }
            else {
                final WebRequest webRequest = new WebRequest();
                webRequest.setUrl("https://explorer.bitcoinverde.org/api/v1/search");
                webRequest.addGetParam("query", blockHash);
                webRequest.addGetParam("rawFormat", "1");

                final HttpResponse response = webRequest.execute();
                final Json result = response.getJsonResult();

                final String blockHex = result.getString("object");
                final ByteArray blockBytes = ByteArray.fromHexString(blockHex);
                block = blockInflater.fromBytes(blockBytes);

                IoUtil.putFileContents(blockDirectory, blockBytes);
            }

            final BlockHeader blockHeader = new ImmutableBlockHeader(block);
            initBlockHeaders.add(blockHeader);
            initBlocks.add(block);

            if (! USE_P2P_BROADCAST) {
                _blockSender.sendBlock(block, null, null, false);
            }
        }

        Logger.debug(initBlockHeaders.getCount() + " blocks loaded.");
        Logger.debug("HeadBlock=" + initBlockHeaders.get(initBlockHeaders.getCount() - 1).getHash());

        return new Tuple<>(initBlockHeaders, initBlocks);
    }

    public Main() {
        if (IS_BITCOIN_VERDE_NODE) {
            _rpcAddress = new BitcoinNodeRpcAddress("localhost", 18334);
            _rpcCredentials = null;
        }
        else {
            // bitcoin.conf:
            //  server=1
            //  txindex=1
            //  zmqpubhashblock=tcp://0.0.0.0:8433
            //  minrelaytxfee=0 // Requires custom BCHN code.
            //  rpcauth=root:b971ece882a77bff1a4803c5e7b418fc$a242915ce44f887e8c28b42cfdd87592d1abffa47084e4fb7718dc982c80636a

            _rpcAddress = new BitcoinNodeRpcAddress("localhost", 8332);
            _rpcCredentials = new RpcCredentials("root", "luaDH5Orq8oTJUJhxz2LP4OV1qlCu62OBl26xDhz8Lk=");
        }
    }

    public void run() {
        final int defaultScenarioBlockCount = (1 + 144 + 100 + 10 + 5 + 2 + 5 + 10); // 277

        final File blocksBaseDirectory = new File("data/blocks");
        final File defaultScenarioDirectory = new File(blocksBaseDirectory, "default");

        final Tuple<MutableList<BlockHeader>, MutableList<Block>> tuple = _loadInitBlocks(blocksBaseDirectory);
        final List<BlockHeader> initBlockHeaders = tuple.first;
        final ConcurrentHashMap<Sha256Hash, Block> initBlocks = new ConcurrentHashMap<>();
        for (final Block block : tuple.second) {
            final Sha256Hash blockHash = block.getHash();
            initBlocks.put(blockHash, block);
        }

        final MutableList<BlockHeader> blockHeaders = new MutableList<>(0);
        blockHeaders.addAll(initBlockHeaders);

        final File manifestFile = new File(defaultScenarioDirectory, "manifest.json");
        final Json reorgBlocksManifestJson;
        final Json blocksManifestJson;
        final Integer manifestBlockCount;
        if (manifestFile.exists()) {
            final Json manifestJson = Json.parse(StringUtil.bytesToString(IoUtil.getFileContents(manifestFile)));
            blocksManifestJson = manifestJson.get("blocks");
            reorgBlocksManifestJson = manifestJson.get("reorgBlocks");
            manifestBlockCount = blocksManifestJson.length();
        }
        else {
            reorgBlocksManifestJson = null;
            blocksManifestJson = null;
            manifestBlockCount = 0;
        }

        _blockSender = new BlockSender(defaultScenarioDirectory, initBlocks, USE_P2P_BROADCAST, _rpcAddress, _rpcCredentials, IS_BITCOIN_VERDE_NODE);
        _blockSender.setEnabled(!SKIP_SEND);
        _blockSender.setPreRelayPercent(PRE_RELAY_PERCENT);

        final int initBlockCount = initBlockHeaders.getCount();
        final boolean allBlocksAreMined = ((manifestBlockCount + initBlockCount) >= defaultScenarioBlockCount);
        if (allBlocksAreMined) {
            // send pre-mined blocks
            _blockSender.transmitTestBlocks(blockHeaders, initBlockCount, blocksManifestJson, manifestBlockCount, reorgBlocksManifestJson, defaultScenarioDirectory);
        }
        else {
            // mine remaining blocks
            final TestBlockMiner testBlockMiner = new TestBlockMiner(_blockSender, manifestFile, defaultScenarioDirectory, !SKIP_MINING);
            testBlockMiner.mine(blockHeaders, blocksManifestJson, manifestBlockCount, reorgBlocksManifestJson);
        }
    }

    @Override
    public void close() {
        _blockSender.close();
    }

}