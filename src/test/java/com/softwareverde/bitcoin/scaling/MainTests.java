package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.BlockInflater;
import com.softwareverde.bitcoin.rpc.BitcoinNodeRpcAddress;
import com.softwareverde.bitcoin.rpc.BitcoinVerdeRpcConnector;
import com.softwareverde.bitcoin.server.main.BitcoinConstants;
import com.softwareverde.bitcoin.transaction.Transaction;
import com.softwareverde.bitcoin.util.ByteUtil;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;
import com.softwareverde.cryptography.hash.sha256.Sha256Hash;
import com.softwareverde.logging.LineNumberAnnotatedLog;
import com.softwareverde.logging.LogLevel;
import com.softwareverde.logging.Logger;
import com.softwareverde.util.IoUtil;
import org.junit.Test;

import java.io.File;

public class MainTests {

    @Test
    public void foo() throws Exception {
        BitcoinConstants.setBlockMaxByteCount((int) (256L * ByteUtil.Unit.Si.MEGABYTES));

        Logger.setLog(LineNumberAnnotatedLog.getInstance());
        Logger.setLogLevel(LogLevel.DEBUG);
        Logger.setLogLevel("com.softwareverde.util", LogLevel.ERROR);
        Logger.setLogLevel("com.softwareverde.network", LogLevel.INFO);
        Logger.setLogLevel("com.softwareverde.async.lock", LogLevel.WARN);
        Logger.setLogLevel("com.softwareverde.bitcoin.stratum.BitcoinCoreStratumServer", LogLevel.WARN);
        Logger.setLogLevel("com.softwareverde.bitcoin.wallet.Wallet", LogLevel.WARN);
        Logger.setLogLevel("com.softwareverde.network.socket.BinarySocketWriteThread", LogLevel.ON);

        final Main main = new Main() {
            @Override
            protected void _sendBlock(final Block block) {
                // Nothing.
            }
        };
        // main.run();

        final File blocksDir = new File("out/data/blocks/default/");
        final Sha256Hash blockToSpendHash = Sha256Hash.fromHexString("68E4159AE6FBD17AAC31C6FB465892341F07569398E86CD510FA739CE595EA51");
        final Long blockHeightToSpend = 257L;
        final Long blockHeight = 259L;

        final MutableList<Main.TransactionWithBlockHeight> transactionsToSpend;
        {
            final Block blockToSpend = main._loadBlock(blockToSpendHash, blocksDir);
            final int transactionCount = blockToSpend.getTransactionCount();
            Logger.info("transactionCount=" + transactionCount);
            transactionsToSpend = new MutableList<>(transactionCount - 1);

            final List<Transaction> transactions = blockToSpend.getTransactions();
            for (int i = 1; i < transactionCount; ++i) {
                final Transaction transaction = transactions.get(i);
                transactionsToSpend.add(new Main.TransactionWithBlockHeight(transaction, blockHeightToSpend));
            }
        }

        final List<Transaction> transactions = main._createSteadyStateTransactions(transactionsToSpend, blockHeight);
        System.out.println(transactions.getCount());
    }
}
