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

    private static final boolean CHECK_RC_ON_MODIFY = true;

    private final ByteBuffer m_buf;
    private final int m_offs;
    private final int m_size;
    private volatile int m_retainCount;

    protected abstract void finalRelease();

    public RetainableByteBuffer( ByteBuffer buf, int offs, int size )
    {
        m_buf = buf;
        m_offs = offs;
        m_size = size;
        m_retainCount = 1;
        m_buf.position( offs );
        m_buf.limit( offs + size );
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
                    m_retainCount = 1;
                    finalRelease();
                }
                break;
            }
        }
    }

    public final ByteBuffer getByteBuffer( SessionImpl.RetainableByteBufferFriend friend )
    {
        /* Supposed to be called only by JS-Collider framework. */
        return m_buf;
    }

    public final int capacity()
    {
        return m_size;
    }

    public final RetainableByteBuffer clear()
    {
        if (CHECK_RC_ON_MODIFY)
            assert( m_retainCount == 1 );

        m_buf.position( m_offs );
        m_buf.limit( m_offs + m_size );
        return this;
    }

    public final RetainableByteBuffer flip()
    {
        if (CHECK_RC_ON_MODIFY)
            assert( m_retainCount == 1 );

        final int position = m_buf.position();
        m_buf.position( m_offs );
        m_buf.limit( position );
        return this;
    }

    public final int limit()
    {
        return (m_buf.limit() - m_offs);
    }

    public final RetainableByteBuffer limit( int limit )
    {
        if (CHECK_RC_ON_MODIFY)
            assert( m_retainCount == 1 );

        if ((limit < 0) || (limit > m_size))
            throw new IllegalArgumentException();
        m_buf.limit( m_offs + limit );
        return this;
    }

    public final int position()
    {
        return (m_buf.position() - m_offs);
    }

    public final RetainableByteBuffer position( int position )
    {
        if (CHECK_RC_ON_MODIFY)
            assert( m_retainCount == 1 );

        if ((position < 0) || (position > limit()))
            throw new IllegalArgumentException();
        m_buf.position( m_offs + position );
        return this;
    }

    public final int remaining()
    {
        return m_buf.remaining();
    }

    public final RetainableByteBuffer put( ByteBuffer src )
    {
        if (CHECK_RC_ON_MODIFY)
            assert( m_retainCount == 1 );

        m_buf.put( src );
        return this;
    }

    public final RetainableByteBuffer put( RetainableByteBuffer src )
    {
        if (CHECK_RC_ON_MODIFY)
            assert( m_retainCount == 1 );

        m_buf.put( src.m_buf );
        return this;
    }

    public final RetainableByteBuffer putInt( int index, int value )
    {
        if (CHECK_RC_ON_MODIFY)
            assert( m_retainCount == 1 );
        m_buf.putInt( index+m_offs, value );
        return this;
    }

    public final RetainableByteBuffer putShort( int index, short value )
    {
        if (CHECK_RC_ON_MODIFY)
            assert( m_retainCount == 1 );
        m_buf.putShort( index+m_offs, value );
        return this;
    }

    public final RetainableByteBuffer putShort( short value )
    {
        if (CHECK_RC_ON_MODIFY)
            assert( m_retainCount == 1 );
        m_buf.putShort( value );
        return this;
    }

    public String toString()
    {
        String ret = super.toString();
        ret += " [" + m_offs + ", " + m_size + "]";
        return ret;
    }
}
