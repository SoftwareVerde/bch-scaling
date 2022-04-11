package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.bip.TestNet4UpgradeSchedule;
import com.softwareverde.bitcoin.bip.UpgradeSchedule;
import com.softwareverde.bitcoin.block.Block;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.block.header.difficulty.work.BlockWork;
import com.softwareverde.bitcoin.block.header.difficulty.work.ChainWork;
import com.softwareverde.bitcoin.block.header.difficulty.work.MutableChainWork;
import com.softwareverde.bitcoin.block.validator.difficulty.AsertReferenceBlock;
import com.softwareverde.bitcoin.block.validator.difficulty.DifficultyCalculator;
import com.softwareverde.bitcoin.chain.time.MedianBlockTime;
import com.softwareverde.bitcoin.chain.time.MutableMedianBlockTime;
import com.softwareverde.bitcoin.context.DifficultyCalculatorContext;
import com.softwareverde.bitcoin.server.main.BitcoinConstants;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;

public class PrivateTestNetDifficultyCalculatorContext implements DifficultyCalculatorContext {
    protected final UpgradeSchedule _upgradeSchedule;
    protected final List<Block> _blocks;

    public PrivateTestNetDifficultyCalculatorContext(final List<Block> blocks) {
        _blocks = blocks;
        _upgradeSchedule = new TestNet4UpgradeSchedule();
    }

    @Override
    public DifficultyCalculator newDifficultyCalculator() {
        return new DifficultyCalculator(this);
    }

    @Override
    public AsertReferenceBlock getAsertReferenceBlock() {
        return BitcoinConstants.TestNet4.asertReferenceBlock;
    }

    @Override
    public BlockHeader getBlockHeader(final Long blockHeight) {
        final int blockIndex = blockHeight.intValue();
        return _blocks.get(blockIndex);
    }

    @Override
    public ChainWork getChainWork(final Long blockHeight) {
        final MutableChainWork chainWork = new MutableChainWork();

        for (int i = 1; i <= blockHeight; ++i) {
            final Block block = _blocks.get(i);
            final Difficulty difficulty = block.getDifficulty();
            final BlockWork blockWork = difficulty.calculateWork();
            chainWork.add(blockWork);
        }

        return chainWork;
    }

    @Override
    public MedianBlockTime getMedianBlockTime(final Long blockHeight) {
        final MutableMedianBlockTime medianBlockTime = new MutableMedianBlockTime();
        if (blockHeight <= 0L) { return medianBlockTime; } // Special case for Genesis block...

        final MutableList<BlockHeader> blockHeadersInDescendingOrder = new MutableList<>(MedianBlockTime.BLOCK_COUNT);

        for (int i = 0; i < MedianBlockTime.BLOCK_COUNT; ++i) {
            final int blockIndex = (int) (blockHeight - i);
            if (blockIndex < 0) { break; }

            final BlockHeader block = _blocks.get(blockIndex);
            blockHeadersInDescendingOrder.add(block);
        }

        // Add the blocks to the MedianBlockTime in ascending order (lowest block-height is added first)...
        final int blockHeaderCount = blockHeadersInDescendingOrder.getCount();
        for (int i = 0; i < blockHeaderCount; ++i) {
            final BlockHeader blockHeader = blockHeadersInDescendingOrder.get(blockHeaderCount - i - 1);
            medianBlockTime.addBlock(blockHeader);
        }

        return medianBlockTime;
    }

    @Override
    public UpgradeSchedule getUpgradeSchedule() {
        return _upgradeSchedule;
    }
}
