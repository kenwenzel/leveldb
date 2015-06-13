package org.iq80.leveldb.util;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

import sun.nio.ch.DirectBuffer;
import sun.nio.ch.FileChannelImpl;

import org.iq80.leveldb.MemoryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("restriction")
public final class ByteBuffers
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ByteBuffers.class);

    private ByteBuffers()
    {
    }

    interface BufferUtil
    {
        int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey);

        void putZero(ByteBuffer dst, int length);

        int compare(ByteBuffer buffer1, int offset1, int length1, ByteBuffer buffer2, int offset2, int length2);

        ByteBufferCrc32 crc32();
    }

    private static final BufferUtil UTIL = PureJavaUtil.INSTANCE;

    private enum PureJavaUtil
            implements BufferUtil
    {
        INSTANCE;

        @Override
        public int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey)
        {
            int sharedKeyBytes = 0;
            final int lpos = leftKey.position(), rpos = rightKey.position();

            if (leftKey != null && rightKey != null) {
                int minSharedKeyBytes = Math.min(leftKey.remaining(), rightKey.remaining());
                while (sharedKeyBytes < minSharedKeyBytes
                        && leftKey.get(lpos + sharedKeyBytes) == rightKey.get(rpos + sharedKeyBytes)) {
                    sharedKeyBytes++;
                }
            }

            return sharedKeyBytes;
        }

        @Override
        public void putZero(ByteBuffer dst, int length)
        {
            for (; length > 0; length--) {
                dst.put((byte) 0);
            }
        }

        @Override
        public int compare(ByteBuffer buffer1, int offset1, int length1, ByteBuffer buffer2, int offset2, int length2)
        {
            if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
                return 0;
            }
            final int len = Math.min(length1, length2);
            for (int i = 0; i < len; i++) {
                int a = (buffer1.get(offset1 + i) & 0xff);
                int b = (buffer2.get(offset2 + i) & 0xff);
                if (a != b) {
                    return a - b;
                }
            }
            return length1 - length2;
        }

        @Override
        public ByteBufferCrc32 crc32()
        {
            return new PureJavaCrc32C();
        }
    }

    public static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0).order(ByteOrder.LITTLE_ENDIAN);

    private static final boolean directBufferSupport;
    static {
        boolean success = false;
        try {
            ((DirectBuffer) ByteBuffer.allocateDirect(8)).cleaner().clean();
            success = true;
        }
        catch (Throwable t) {
            LOGGER.debug("failed to access DirectBuffer support", t);
            success = false;
        }
        finally {
            LOGGER.debug("DirectBuffer support:", success);
            directBufferSupport = success;
        }
    }

    public static void freeDirect(ByteBuffer buffer)
    {
        if (directBufferSupport && buffer.isDirect()) {
            ((DirectBuffer) buffer).cleaner().clean();
        }
        // else
        // leave it to Java GC
    }

    private static final Method unmap;
    static {
        Method x;
        try {
            x = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            x.setAccessible(true);
        }
        catch (Throwable t) {
            LOGGER.debug("failed to access MappedByteBuffer support", t);
            x = null;
        }
        unmap = x;
    }

    public static void unmap(MappedByteBuffer buffer)
            throws IOException
    {
        if (unmap != null) {
            try {
                unmap.invoke(null, buffer);
            }
            catch (Exception e) {
                throw new IOException("Failed to unmap MappedByteBuffer", e);
            }
        }
        // else
        // leave it to Java GC
    }

    public static void writeLengthPrefixedBytes(ByteBuffer dst, ByteBuffer src)
    {
        VariableLengthQuantity.writeVariableLengthInt(src.remaining(), dst);
        dst.put(src);
    }

    public static int getRemaining(ByteBuffer[] bufs)
    {
        int size = 0;
        for(ByteBuffer buf : bufs)
        {
            size+=buf.remaining();
        }
        return size;
    }

    public static void writeLengthPrefixedBytes(GrowingBuffer buffer, ByteBuffer src)
    {
        VariableLengthQuantity.writeVariableLengthInt(src.remaining(), buffer);
        buffer.put(src);
    }

    public static void writeLengthPrefixedBytesTransparent(GrowingBuffer buffer, ByteBuffer src)
    {
        src.mark();
        writeLengthPrefixedBytes(buffer, src);
        src.reset();
    }

    public static void writeLengthPrefixedBytesTransparent(ByteBuffer buffer, ByteBuffer src)
    {
        src.mark();
        writeLengthPrefixedBytes(buffer, src);
        src.reset();
    }

    public static void writeLengthPrefixedBytes(GrowingBuffer buffer, ByteBuffer[] srcs)
    {
        VariableLengthQuantity.writeVariableLengthInt(getRemaining(srcs), buffer);
        for (ByteBuffer src : srcs) {
            buffer.put(src);
        }
    }

    public static ByteBuffer readLengthPrefixedBytes(ByteBuffer src)
    {
        int length = VariableLengthQuantity.readVariableLengthInt(src);
        return duplicateAndAdvance(src, length);
    }

    /**
     * buffer duplication which preserves byte order
     */
    public static ByteBuffer duplicate(ByteBuffer src)
    {
        return src.duplicate().order(src.order());
    }

    public static ByteBuffer duplicate(ByteBuffer src, int position, int limit)
    {
        ByteBuffer ret = duplicate(src);
        ret.limit(limit).position(position);
        return ret;
    }

    public static ByteBuffer duplicateByLength(ByteBuffer src, int position, int length)
    {
        return duplicate(src, position, position + length);
    }

    public static ByteBuffer slice(ByteBuffer src)
    {
        return src.slice().order(src.order());
    }

    public static int readUnsignedByte(ByteBuffer src)
    {
        return src.get() & 0xFF;
    }

    public static int getUnsignedByte(ByteBuffer src, int position)
    {
        return src.get(position) & 0xFF;
    }

    public static ByteBuffer duplicateAndAdvance(ByteBuffer src, int length)
    {
        final int oldpos = src.position();
        src.position(oldpos + length);
        return duplicate(src, oldpos, oldpos + length);
    }

    public static int calculateSharedBytes(ByteBuffer leftKey, ByteBuffer rightKey)
    {
        return UTIL.calculateSharedBytes(leftKey, rightKey);
    }

    public static ByteBuffer putZero(ByteBuffer dst, int length)
    {
        UTIL.putZero(dst, length);
        return dst;
    }

    public static int compare(ByteBuffer a, ByteBuffer b)
    {
        return UTIL.compare(a, a.position(), a.remaining(), b, b.position(), b.remaining());
    }

    public static ByteBuffer copy(ByteBuffer src, int length, MemoryManager memory)
    {
        ByteBuffer ret = memory.allocate(length).put(duplicateByLength(src, src.position(), length));
        ret.rewind();
        return ret;
    }

    public static ByteBuffer copy(ByteBuffer src, MemoryManager memory)
    {
        return copy(src, src.remaining(), memory);
    }

    public static byte[] toArray(ByteBuffer src, byte[] dst)
    {
        src.mark();
        src.get(dst);
        src.reset();
        return dst;
    }

    public static byte[] toArray(ByteBuffer src)
    {
        return toArray(src, new byte[src.remaining()]);
    }

    public static String toString(ByteBuffer src)
    {
        return new String(toArray(src), StandardCharsets.UTF_8);
    }

    /**
     * @return dst
     */
    public static ByteBuffer putLength(ByteBuffer dst, ByteBuffer src, int length)
    {
        int oldlim = src.limit();
        src.limit(src.position() + length);
        dst.put(src);
        src.limit(oldlim);
        return dst;
    }

    public static ByteBufferCrc32 crc32()
    {
        return UTIL.crc32();
    }

    private static final int MASK_DELTA = 0xa282ead8;

    /**
     * Return a masked representation of crc.
     * <p/>
     * Motivation: it is problematic to compute the CRC of a string that
     * contains embedded CRCs.  Therefore we recommend that CRCs stored
     * somewhere (e.g., in files) should be masked before being stored.
     */
    public static int maskChecksum(int crc)
    {
        // Rotate right by 15 bits and add a constant.
        return ((crc >>> 15) | (crc << 17)) + MASK_DELTA;
    }

    /**
     * Return the crc whose masked representation is masked_crc.
     */
    public static int unmaskChecksum(int maskedCrc)
    {
        int rot = maskedCrc - MASK_DELTA;
        return ((rot >>> 17) | (rot << 15));
    }
}
