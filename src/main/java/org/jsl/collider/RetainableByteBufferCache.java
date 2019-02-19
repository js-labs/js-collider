/*
 * Copyright (C) 2013 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of JS-Collider framework.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.jsl.collider;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class RetainableByteBufferCache extends ObjectCache<RetainableByteBuffer>
{
    private static class BufferImpl extends RetainableByteBufferImpl
    {
        private RetainableByteBufferCache m_cache;

        BufferImpl(ByteBuffer buf, RetainableByteBufferCache cache)
        {
            super(buf);
            m_cache = cache;
        }

        protected void finalRelease()
        {
            reinit();
            m_cache.put(this);
        }
    }

    private final boolean m_useDirectBuffer;
    private final int m_bufferCapacity;
    private final ByteOrder m_byteOrder;

    protected BufferImpl allocateObject()
    {
        final ByteBuffer byteBuffer =
                m_useDirectBuffer
                    ? ByteBuffer.allocateDirect(m_bufferCapacity)
                    : ByteBuffer.allocate(m_bufferCapacity);
        byteBuffer.order(m_byteOrder);
        return new BufferImpl(byteBuffer, this);
    }

    public RetainableByteBufferCache(boolean useDirectBuffer, int bufferCapacity, ByteOrder byteOrder, int size)
    {
        super(RetainableByteBufferCache.class.getSimpleName() + "-" + bufferCapacity, new RetainableByteBuffer[size]);
        m_useDirectBuffer = useDirectBuffer;
        m_bufferCapacity = bufferCapacity;
        m_byteOrder = byteOrder;
    }
}
