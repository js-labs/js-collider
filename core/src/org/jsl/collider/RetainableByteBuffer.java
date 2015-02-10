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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class RetainableByteBuffer
{
    private final static AtomicIntegerFieldUpdater<RetainableByteBuffer> s_retainCountUpdater =
            AtomicIntegerFieldUpdater.newUpdater( RetainableByteBuffer.class, "m_retainCount" );

    /* It looks like a good idea having a possibility
     * to link RetainableByteBuffers to each other
     * (adding something like a 'next' member),
     * but it is not, at least because we can enqueue same
     * retainable byte buffer to the multiple sessions.
     */

    private final ByteBuffer m_buf;
    private final int m_offset;
    private final int m_length;
    private volatile int m_retainCount;

    private final static class Slice extends RetainableByteBuffer
    {
        private final RetainableByteBuffer m_rbuf;

        protected void finalRelease()
        {
            m_rbuf.release();
        }

        public Slice( RetainableByteBuffer rbuf )
        {
            super( rbuf.m_buf.slice(), 0, rbuf.m_buf.remaining() );
            m_rbuf = rbuf;
            rbuf.retain();
        }
    }

    protected abstract void finalRelease();

    public RetainableByteBuffer( ByteBuffer buf, int offset, int length )
    {
        m_buf = buf;
        m_offset = offset;
        m_length = length;
        m_retainCount = 1;
        m_buf.position( offset );
        m_buf.limit( offset + length );
    }

    public final void retain()
    {
        for (;;)
        {
            final int retainCount = m_retainCount;
            assert( retainCount > 0 );
            if (s_retainCountUpdater.compareAndSet(this, retainCount, retainCount+1))
                break;
        }
    }

    public final RetainableByteBuffer slice()
    {
        final int position = m_buf.position();
        if ((position == m_offset) &&
            ((m_buf.limit() - position) == m_length))
        {
            retain();
            return this;
        }
        return new Slice( this );
    }

    public final void release()
    {
        for (;;)
        {
            final int retainCount = m_retainCount;
            assert( retainCount > 0 );
            final int newValue = (retainCount - 1);
            if (s_retainCountUpdater.compareAndSet(this, retainCount, newValue))
            {
                if (newValue == 0)
                {
                    /* The instance can be reused with cache,
                     * so it is better to set retain count = 1
                     * before finalRelease() call.
                     */
                    m_retainCount = 1;
                    finalRelease();
                }
                break;
            }
        }
    }

    public final ByteBuffer getByteBuffer()
    {
        return m_buf;
    }

    public final int capacity()
    {
        return m_length;
    }

    public final RetainableByteBuffer clear()
    {
        m_buf.position( m_offset );
        m_buf.limit( m_offset + m_length );
        return this;
    }

    public final RetainableByteBuffer reset()
    {
        m_buf.position( m_offset );
        m_buf.limit( m_offset + m_length );
        return this;
    }

    public final RetainableByteBuffer flip()
    {
        final int position = m_buf.position();
        m_buf.position( m_offset );
        m_buf.limit( position );
        return this;
    }

    public final int limit()
    {
        return (m_buf.limit() - m_offset);
    }

    public final RetainableByteBuffer limit( int limit )
    {
        if ((limit < 0) || (limit > m_length))
            throw new IllegalArgumentException();
        m_buf.limit( m_offset + limit );
        return this;
    }

    public final int position()
    {
        return (m_buf.position() - m_offset);
    }

    public final RetainableByteBuffer position( int position )
    {
        if ((position < 0) || (position > limit()))
            throw new IllegalArgumentException();
        m_buf.position( m_offset + position );
        return this;
    }

    public final int remaining()
    {
        return m_buf.remaining();
    }

    public final RetainableByteBuffer put( ByteBuffer src )
    {
        m_buf.put( src );
        return this;
    }

    public final RetainableByteBuffer put( RetainableByteBuffer src )
    {
        m_buf.put( src.m_buf );
        return this;
    }

    public final RetainableByteBuffer putInt( int value )
    {
        m_buf.putInt( value );
        return this;
    }

    public final RetainableByteBuffer putInt( int index, int value )
    {
        m_buf.putInt( index+m_offset, value );
        return this;
    }

    public final int getInt()
    {
        return m_buf.getInt();
    }

    public final int getInt( int index )
    {
        return m_buf.getInt( m_offset+index );
    }

    public final RetainableByteBuffer putShort( short value )
    {
        m_buf.putShort( value );
        return this;
    }

    public final RetainableByteBuffer putShort( int index, short value )
    {
        m_buf.putShort( index+m_offset, value );
        return this;
    }

    public final RetainableByteBuffer putDouble( double value )
    {
        m_buf.putDouble( value );
        return this;
    }

    public final RetainableByteBuffer putDouble( int index, double value )
    {
        m_buf.putDouble( index+m_offset, value );
        return this;
    }

    public String toString()
    {
        String ret = super.toString();
        ret += " [" + m_offset + ", " + m_length + "]";
        return ret;
    }
}
