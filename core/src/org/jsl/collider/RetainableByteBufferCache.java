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

public class RetainableByteBufferCache extends ObjectCache<RetainableByteBuffer>
{
    private class BufferImpl extends RetainableByteBufferImpl
    {
        public BufferImpl( ByteBuffer buf )
        {
            super( buf );
        }

        protected void finalRelease()
        {
            super.finalRelease();
            RetainableByteBufferCache.this.put( this );
        }
    }

    private final int m_bufferCapacity;
    private final boolean m_useDirectBuffer;

    protected BufferImpl allocateObject()
    {
        final ByteBuffer buf = 
                m_useDirectBuffer
                    ? ByteBuffer.allocateDirect( m_bufferCapacity )
                    : ByteBuffer.allocate( m_bufferCapacity );
        return new BufferImpl( buf );
    }

    public RetainableByteBufferCache( int bufferCapacity, boolean useDirectBuffer, int maxSize, int initialSize )
    {
        super( RetainableByteBuffer.class.getSimpleName() + "-" + bufferCapacity, new RetainableByteBuffer[maxSize] );
        m_bufferCapacity = bufferCapacity;
        m_useDirectBuffer = useDirectBuffer;
        for (int idx=0; idx<initialSize; idx++)
            put( allocateObject() );
    }
}
