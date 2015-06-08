/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.iq80.leveldb.table;

import org.iq80.leveldb.Options;
import org.iq80.leveldb.util.ByteBuffers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Comparator;

public class FileChannelTable
        extends Table
{
    public FileChannelTable(String name, FileChannel fileChannel, Comparator<ByteBuffer> comparator, Options options)
            throws IOException
    {
        super(name, fileChannel, comparator, options.verifyChecksums(), options.memoryManager());
    }

    @Override
    protected Footer init()
            throws IOException
    {
        long size = fileChannel.size();
        ByteBuffer footerData = read(size - Footer.ENCODED_LENGTH, Footer.ENCODED_LENGTH);
        return Footer.readFooter(footerData);
    }

    @SuppressWarnings({"AssignmentToStaticFieldFromInstanceMethod", "NonPrivateFieldAccessedInSynchronizedContext"})
    @Override
    protected Block readBlock(BlockHandle blockHandle)
            throws IOException
    {
        // read block trailer
        ByteBuffer readBuffer = read(blockHandle.getOffset(), blockHandle.getDataSize() + BlockTrailer.ENCODED_LENGTH);
        ByteBuffer uncompressedBuffer = ByteBuffers.duplicateAndAdvance(readBuffer, blockHandle.getDataSize());
        BlockTrailer blockTrailer = BlockTrailer.readBlockTrailer(readBuffer);

// todo re-enable crc check when ported to support direct buffers
//        // only verify check sums if explicitly asked by the user
//        if (verifyChecksums) {
//            // checksum data and the compression type in the trailer
//            PureJavaCrc32C checksum = new PureJavaCrc32C();
//            checksum.update(data.getRawArray(), data.getRawOffset(), blockHandle.getDataSize() + 1);
//            int actualCrc32c = checksum.getMaskedValue();
//
//            Preconditions.checkState(blockTrailer.getCrc32c() == actualCrc32c, "Block corrupted: checksum mismatch");
//        }

        // decompress data

        ByteBuffer uncompressedData;
        // if (blockTrailer.getCompressionType() == SNAPPY) {
        // synchronized (FileChannelTable.class) {
        // int uncompressedLength = uncompressedLength(uncompressedBuffer);
        // if (uncompressedScratch.capacity() < uncompressedLength) {
        // uncompressedScratch = ByteBuffer.allocateDirect(uncompressedLength);
        // }
        // uncompressedScratch.clear();
        //
        // Snappy.uncompress(uncompressedBuffer, uncompressedScratch);
        // uncompressedData = Slices.copiedBuffer(uncompressedScratch);
        // }
        // }
        // else {
        uncompressedData = uncompressedBuffer;
        // }

        return new Block(uncompressedData, comparator, memory);
    }

    private ByteBuffer read(long offset, int length)
            throws IOException
    {
        ByteBuffer uncompressedBuffer = memory.allocate(length);
        fileChannel.read(uncompressedBuffer, offset);
        if (uncompressedBuffer.hasRemaining()) {
            throw new IOException("Could not read all the data");
        }
        uncompressedBuffer.rewind();
        return uncompressedBuffer;
    }
}
