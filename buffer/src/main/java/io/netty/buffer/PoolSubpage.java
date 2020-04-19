/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.buffer;

/**
 * Page 切分为多个 Subpage 的情况
 */
final class PoolSubpage<T> implements PoolSubpageMetric {

    /**
     * 所属 PoolChunk 对象
     */
    final PoolChunk<T> chunk;
    /**
     * 所属Page的标号（在 {@link PoolChunk}中 memoryMap 的节点编号）
     */
    private final int memoryMapIdx;
    /**
     * 整个chunk偏移的字节数，通过 PoolChunk#runOffset(id) 计算得到
     *
     */
    private final int runOffset;
    /**
     * Page页大小
     */
    private final int pageSize;
    /**
     * <p>
     * Subpage 分配信息数组
     * </p>
     * <pre>
     * 每个 long 的 bits 位代表一个 Subpage 是否分配；
     * 因为 PoolSubpage 可能会超过 64 个( long 的 bits 位数 )，所以使用数组
     *
     * 例如：Page 默认大小为 8KB ，Subpage 默认最小为 16 B ，
     * 所以一个 Page 最多可包含 8 * 1024 / 16 = 512 个 Subpage，
     * 因此，bitmap 数组大小为 512 / 64 = 8
     *
     * 另外，bitmap 的数组大小，使用 {@link #bitmapLength} 来标记。
     * 或者说，bitmap 数组，默认按照 Subpage 的大小为 16B 来初始化。
     * 为什么是这样的设定呢？因为 PoolSubpage 可重用，
     * 通过 {@link #init(PoolSubpage, int)} 进行重新初始化。
     * </pre>
     *
     */
    private final long[] bitmap;
    /**
     * 双向链表，前一个 PoolSubpage 对象（相同内存规格的Subpage对象）
     */
    PoolSubpage<T> prev;
    /**
     * 双向链表，后一个 PoolSubpage 对象（相同内存规格的Subpage对象）
     */
    PoolSubpage<T> next;

    /**
     * 是否未销毁
     */
    boolean doNotDestroy;
    /**
     * 均等切分的大小，即每个 Subpage 的占用内存大小
     */
    int elemSize;
    /**
     * 最多可以切分的小块数
     * <pre>
     *     16B 为 512 个
     *     32b 为 256 个
     * </pre>
     *
     * @see #bitmap 分配信息数组
     */
    private int maxNumElems;
    /**
     * 位图信息长度，long的个数
     */
    private int bitmapLength;
    /**
     * 下一个可分配的小块位置信息
     */
    private int nextAvail;
    /**
     * 剩余可用 Subpage 的数量
     */
    private int numAvail;

    // TODO: Test if adding padding helps under contention
    //private long pad0, pad1, pad2, pad3, pad4, pad5, pad6, pad7;

    /**
     * 双向链表，头节点
     * <br />
     * Special constructor that creates a linked list head
     * */
    PoolSubpage(int pageSize) {
        chunk = null;
        memoryMapIdx = -1;
        runOffset = -1;
        elemSize = -1;
        this.pageSize = pageSize;
        bitmap = null;
    }

    /**
     * 双向链表，Page 节点
     */
    PoolSubpage(PoolSubpage<T> head, PoolChunk<T> chunk, int memoryMapIdx, int runOffset, int pageSize, int elemSize) {
        this.chunk = chunk;
        this.memoryMapIdx = memoryMapIdx;
        this.runOffset = runOffset;
        this.pageSize = pageSize; // 默认 8K == 8192
        // 此处使用最大值，最小分配16B * 64(long包含64位)所需的long个数
        // bitmap数组可以重用
        bitmap = new long[pageSize >>> 10]; // pageSize / 16 / 64 == 8
        // 初始化
        init(head, elemSize);
    }

    /**
     * 首次初始化或者重新使用
     */
    void init(PoolSubpage<T> head, int elemSize) {
        doNotDestroy = true;
        this.elemSize = elemSize;
        if (elemSize != 0) {
            maxNumElems = numAvail = pageSize / elemSize;
            nextAvail = 0;
            // 计算 bitmapLength 的大小，相当于 maxNumElems / 64(long型数据的bit位数)
            bitmapLength = maxNumElems >>> 6;
            if ((maxNumElems & 63) != 0) {
                bitmapLength ++;// subpage不是64倍，多需要一个long
            }

            for (int i = 0; i < bitmapLength; i ++) {
                // 重置为0，因为回收时也会调用这个方法
                bitmap[i] = 0;
            }
        }
        addToPool(head);
    }

    /**
     * 分配一个 Subpage 内存块，并返回该内存块的位置 handle
     *
     * @return Returns the bitmap index of the subpage allocation.
     */
    long allocate() {
        if (elemSize == 0) {
            // 防御性编程，不存在这种情况
            return toHandle(0);
        }

        if (numAvail == 0 || !doNotDestroy) {
            // 可用数量为 0 ，或者已销毁，返回 -1 ，即不可分配
            return -1;
        }

        // 获得下一个可用的 Subpage 在 bitmap 中的总体位置
        final int bitmapIdx = getNextAvail();
        // == bitmapIdx / 64，获得下一个可用的 Subpage 在 bitmap 中数组的位置
        int q = bitmapIdx >>> 6;// 高24位表示long数组索引
        // == bitmapIdx % 64，获得下一个可用的 Subpage 在 bitmap 中数组的位置的第几 bits
        int r = bitmapIdx & 63;// 低6位表示在long中实际分配的二进制位 63 == 0011 1111
        assert (bitmap[q] >>> r & 1) == 0; // == 0 没有被使用
        // 修改 Subpage 在 bitmap 中不可分配。设置为1
        bitmap[q] |= 1L << r;

        if (-- numAvail == 0) {
            // 从双向链表中移除
            removeFromPool();
        }

        return toHandle(bitmapIdx);
    }

    /**
     * @return {@code true} if this subpage is in use.
     *         {@code false} if this subpage is not used by its chunk and thus it's OK to be released.
     */
    boolean free(PoolSubpage<T> head, int bitmapIdx) {
        if (elemSize == 0) {
            // 防御性编程，不存在这种情况
            return true;
        }
        // 获得 Subpage 在 bitmap 中数组的位置
        int q = bitmapIdx >>> 6;
        // 获得 Subpage 在 bitmap 中数组的位置的第几 bits
        int r = bitmapIdx & 63;
        assert (bitmap[q] >>> r & 1) != 0;
        bitmap[q] ^= 1L << r;

        setNextAvail(bitmapIdx);

        if (numAvail ++ == 0) {
            addToPool(head);
            return true;
        }

        if (numAvail != maxNumElems) {
            return true;
        } else {
            // Subpage not in use (numAvail == maxNumElems)
            if (prev == next) {
                // Do not remove if this subpage is the only one left in the pool.
                return true;
            }

            // Remove this subpage from the pool if there are other subpages left in the pool.
            doNotDestroy = false;
            removeFromPool();
            return false;
        }
    }

    /**
     * 添加当前节点到 Arena 双向链表中
     */
    private void addToPool(PoolSubpage<T> head) {
        assert prev == null && next == null;
        // 将当前节点，插入到 head 和 head.next 中间
        prev = head;
        next = head.next;
        next.prev = this;
        head.next = this;
    }

    private void removeFromPool() {
        assert prev != null && next != null;
        prev.next = next;
        next.prev = prev;
        next = null;
        prev = null;
    }

    private void setNextAvail(int bitmapIdx) {
        nextAvail = bitmapIdx;
    }

    private int getNextAvail() {
        int nextAvail = this.nextAvail;
        if (nextAvail >= 0) {
            this.nextAvail = -1;
            return nextAvail;
        }
        return findNextAvail();
    }

    private int findNextAvail() {
        final long[] bitmap = this.bitmap;
        final int bitmapLength = this.bitmapLength;
        for (int i = 0; i < bitmapLength; i ++) {
            long bits = bitmap[i];
            /*
             * 非运算，每个bit位不全是1，说明至少还有一个可以分配的subPage，
             * 如果每一位bit都是1，则其值等于 -1， ~(-1) == 0
             */
            if (~bits != 0) {
                // 还有可用的均等小块
                return findNextAvail0(i, bits);
            }
        }
        return -1;
    }

    private int findNextAvail0(int i, long bits) {
        final int maxNumElems = this.maxNumElems;
        // 左移6，空出来的位置添加索引j
        final int baseVal = i << 6;

        // 从最低位开始表示分配信息，最低位表示第1块分配，
        // 一个long型数据有64位，所以循环64次
        for (int j = 0; j < 64; j ++) {
            if ((bits & 1) == 0) {
                int val = baseVal | j;
                if (val < maxNumElems) {
                    return val;
                } else {
                    break;
                }
            }
            bits >>>= 1;
        }
        return -1;
    }

    private long toHandle(int bitmapIdx) {
        // 0100 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000‬
        // 为什么会有 0x4000000000000000L 呢？
        // 因为：PoolChunk#allocate(int normCapacity) 中：
        //     - 如果分配的是Page内存块，返回的是 memoryMapIdx
        //     - 如果分配的是 Subpage 内存块，返回的是 handle。但是，如果 bitmapIdx = 0，
        //         那么没有 0x4000000000000000L 情况下，就会和【分配 Page 内存块】冲突。
        //         因此，需要有 0x4000000000000000L
        return 0x4000000000000000L | (long) bitmapIdx << 32 | memoryMapIdx;
    }

    @Override
    public String toString() {
        final boolean doNotDestroy;
        final int maxNumElems;
        final int numAvail;
        final int elemSize;
        if (chunk == null) {
            // This is the head so there is no need to synchronize at all as these never change.
            doNotDestroy = true;
            maxNumElems = 0;
            numAvail = 0;
            elemSize = -1;
        } else {
            synchronized (chunk.arena) {
                if (!this.doNotDestroy) {
                    doNotDestroy = false;
                    // Not used for creating the String.
                    maxNumElems = numAvail = elemSize = -1;
                } else {
                    doNotDestroy = true;
                    maxNumElems = this.maxNumElems;
                    numAvail = this.numAvail;
                    elemSize = this.elemSize;
                }
            }
        }

        if (!doNotDestroy) {
            return "(" + memoryMapIdx + ": not in use)";
        }

        return "(" + memoryMapIdx + ": " + (maxNumElems - numAvail) + '/' + maxNumElems +
                ", offset: " + runOffset + ", length: " + pageSize + ", elemSize: " + elemSize + ')';
    }

    @Override
    public int maxNumElements() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }

        synchronized (chunk.arena) {
            return maxNumElems;
        }
    }

    @Override
    public int numAvailable() {
        if (chunk == null) {
            // It's the head.
            return 0;
        }

        synchronized (chunk.arena) {
            return numAvail;
        }
    }

    @Override
    public int elementSize() {
        if (chunk == null) {
            // It's the head.
            return -1;
        }

        synchronized (chunk.arena) {
            return elemSize;
        }
    }

    @Override
    public int pageSize() {
        return pageSize;
    }

    void destroy() {
        if (chunk != null) {
            chunk.destroy();
        }
    }
}
