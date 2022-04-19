package com.softwareverde.bitcoin.scaling;

import com.softwareverde.bitcoin.bip.IntraNetUpgradeSchedule;
import com.softwareverde.bitcoin.bip.UpgradeSchedule;
import com.softwareverde.bitcoin.block.BlockId;
import com.softwareverde.bitcoin.block.header.BlockHeader;
import com.softwareverde.bitcoin.block.header.difficulty.Difficulty;
import com.softwareverde.bitcoin.block.header.difficulty.work.BlockWork;
import com.softwareverde.bitcoin.block.header.difficulty.work.ChainWork;
import com.softwareverde.bitcoin.block.header.difficulty.work.MutableChainWork;
import com.softwareverde.bitcoin.block.validator.difficulty.AsertReferenceBlock;
import com.softwareverde.bitcoin.block.validator.difficulty.DifficultyCalculator;
import com.softwareverde.bitcoin.chain.segment.BlockchainSegmentId;
import com.softwareverde.bitcoin.chain.time.MedianBlockTime;
import com.softwareverde.bitcoin.chain.time.MutableMedianBlockTime;
import com.softwareverde.bitcoin.context.DifficultyCalculatorContext;
import com.softwareverde.bitcoin.context.core.AsertReferenceBlockLoader;
import com.softwareverde.constable.list.List;
import com.softwareverde.constable.list.mutable.MutableList;

public class PrivateTestNetDifficultyCalculatorContext implements DifficultyCalculatorContext {
    protected final UpgradeSchedule _upgradeSchedule;
    protected final MutableList<BlockHeader> _blocks;

    public PrivateTestNetDifficultyCalculatorContext() {
        _blocks = new MutableList<>(0);
        _upgradeSchedule = new IntraNetUpgradeSchedule();
    }

    public void addBlock(final BlockHeader block) {
        synchronized (_blocks) {
            _blocks.add(block);
        }
    }

    public void addBlocks(final List<BlockHeader> blocks) {
        synchronized (_blocks) {
            _blocks.addAll(blocks);
        }
    }

    @Override
    public DifficultyCalculator newDifficultyCalculator() {
        return new DifficultyCalculator(this);
    }

    @Override
    public AsertReferenceBlock getAsertReferenceBlock() {
        final AsertReferenceBlockLoader asertReferenceBlockLoader = new AsertReferenceBlockLoader(new AsertReferenceBlockLoader.ReferenceBlockLoaderContext() {
            @Override
            public BlockId getHeadBlockIdOfBlockchainSegment(final BlockchainSegmentId blockchainSegmentId) {
                synchronized (_blocks) {
                    final int blockCount = _blocks.getCount();
                    return BlockId.wrap(blockCount - 1L);
                }
            }

            @Override
            public MedianBlockTime getMedianBlockTime(final BlockId blockId) {
                final Long blockHeight = blockId.longValue();
                return PrivateTestNetDifficultyCalculatorContext.this.getMedianBlockTime(blockHeight);
            }

            @Override
            public Long getBlockTimestamp(final BlockId blockId) {
                synchronized (_blocks) {
                    final int blockIndex = (int) blockId.longValue();
                    final BlockHeader block = _blocks.get(blockIndex);
                    return block.getTimestamp();
                }
            }

            @Override
            public Long getBlockHeight(final BlockId blockId) {
                return blockId.longValue();
            }

            @Override
            public BlockId getBlockIdAtHeight(final BlockchainSegmentId blockchainSegmentId, final Long blockHeight) {
                return BlockId.wrap(blockHeight);
            }

            @Override
            public Difficulty getDifficulty(final BlockId blockId) {
                synchronized (_blocks) {
                    final int blockIndex = (int) blockId.longValue();
                    final BlockHeader block = _blocks.get(blockIndex);
                    return block.getDifficulty();
                }
            }

            @Override
            public UpgradeSchedule getUpgradeSchedule() {
                return PrivateTestNetDifficultyCalculatorContext.this.getUpgradeSchedule();
            }
        });

        try {
            return asertReferenceBlockLoader.getAsertReferenceBlock(null);
        }
        catch (final Exception exception) {
            return null;
        }
    }

    @Override
    public BlockHeader getBlockHeader(final Long blockHeight) {
        synchronized (_blocks) {
            final int blockIndex = blockHeight.intValue();
            return _blocks.get(blockIndex);
        }
    }

    @Override
    public ChainWork getChainWork(final Long blockHeight) {
        final MutableChainWork chainWork = new MutableChainWork();

        synchronized (_blocks) {
            for (int i = 1; i <= blockHeight; ++i) {
                final BlockHeader block = _blocks.get(i);
                final Difficulty difficulty = block.getDifficulty();
                final BlockWork blockWork = difficulty.calculateWork();
                chainWork.add(blockWork);
            }
        }

        return chainWork;
    }

    @Override
    public MedianBlockTime getMedianBlockTime(final Long blockHeight) {
        final MutableMedianBlockTime medianBlockTime = new MutableMedianBlockTime();
        if (blockHeight <= 0L) { return medianBlockTime; } // Special case for Genesis block...

        final MutableList<BlockHeader> blockHeadersInDescendingOrder = new MutableList<>(MedianBlockTime.BLOCK_COUNT);

        synchronized (_blocks) {
            for (int i = 0; i < MedianBlockTime.BLOCK_COUNT; ++i) {
                final int blockIndex = (int) (blockHeight - i);
                if (blockIndex < 0) { break; }

                final BlockHeader block = _blocks.get(blockIndex);
                blockHeadersInDescendingOrder.add(block);
            }
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
