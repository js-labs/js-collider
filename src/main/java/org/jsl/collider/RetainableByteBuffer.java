/*
 * Copyright (C) 2015 Sergey Zubarev, info@js-labs.org
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
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public abstract class RetainableByteBuffer
{
    private final static AtomicIntegerFieldUpdater<RetainableByteBuffer> s_retainCountUpdater =
            AtomicIntegerFieldUpdater.newUpdater( RetainableByteBuffer.class, "m_retainCount" );

    protected final ByteBuffer m_buf;
    private volatile int m_retainCount;

    private static class Slice extends RetainableByteBufferImpl
    {
        private final RetainableByteBuffer m_parent;

        Slice(ByteBuffer byteBuffer, RetainableByteBuffer parent)
        {
            super(byteBuffer);
            m_parent = parent;
        }

        protected void finalRelease()
        {
            m_parent.release();
        }
    }

    private static class Impl extends RetainableByteBufferImpl
    {
        Impl(ByteBuffer byteBuffer)
        {
            super(byteBuffer);
        }

        protected void finalRelease()
        {
            /* Do nothing */
        }
    }

    abstract protected void finalRelease();

    protected void reinit()
    {
        /* To be called by derived class if instance going to be reused */
        m_buf.clear();
        assert(s_retainCountUpdater.get(this) == 0);
        s_retainCountUpdater.lazySet(this, 1);
    }

    protected RetainableByteBuffer(ByteBuffer buf)
    {
        m_buf = buf;
        s_retainCountUpdater.lazySet(this, 1);
    }

    public final ByteBuffer getNioByteBuffer()
    {
        return m_buf;
    }

    public final void retain()
    {
        for (;;)
        {
            final int retainCount = s_retainCountUpdater.get(this);
            assert(retainCount > 0);
            if (s_retainCountUpdater.compareAndSet(this, retainCount, retainCount+1))
                break;
        }
    }

    public final void release()
    {
        for (;;)
        {
            final int retainCount = s_retainCountUpdater.get(this);
            assert(retainCount > 0);
            if (s_retainCountUpdater.compareAndSet(this, retainCount, retainCount-1))
            {
                if (retainCount == 1)
                    finalRelease();
                break;
            }
        }
    }

    public final boolean releaseReuse()
    {
        for (;;)
        {
            final int retainCount = s_retainCountUpdater.get(this);
            assert(retainCount > 0);

            if (retainCount == 1)
            {
                /* instance has only one reference
                 * and this reference definitely owned by the caller,
                 * so it can be safely reused.
                 */
                clear();
                return true;
            }
            else if (s_retainCountUpdater.compareAndSet(this, retainCount, retainCount-1))
                return false;
        }
    }

    public final boolean clearSafe()
    {
        final int retainCount = s_retainCountUpdater.get(this);
        if (retainCount == 1)
        {
            /* instance has only one reference and this reference
             * and it definitely owned by the caller,
             * so we can safely clear the instance.
             */
            clear();
            return true;
        }
        else
            return false;
    }

    /*
     * NIO ByteBuffer interface mimic.
     */

    ByteOrder order() { return m_buf.order(); }
    RetainableByteBuffer order(ByteOrder byteOrder) { m_buf.order(byteOrder); return this; }

    public abstract int capacity();
    public abstract RetainableByteBuffer clear();

    public final RetainableByteBuffer duplicate()
    {
        retain();
        return new Slice(m_buf.duplicate(), this);
    }

    public abstract RetainableByteBuffer flip();
    public abstract RetainableByteBuffer rewind();

    public abstract int limit();
    public abstract RetainableByteBuffer limit( int limit );

    public abstract int position();
    public abstract RetainableByteBuffer position( int position );

    public final int remaining()
    {
        return m_buf.remaining();
    }

    public final RetainableByteBuffer reset()
    {
        m_buf.reset();
        return this;
    }

    public final RetainableByteBuffer slice()
    {
        retain();
        return new Slice(m_buf.slice(), this);
    }

    public abstract byte get( int index );
    public abstract RetainableByteBuffer put( int index, byte value );

    public abstract int getInt( int index );
    public abstract RetainableByteBuffer putInt( int index, int value );

    public abstract short getShort( int index );
    public abstract RetainableByteBuffer putShort( int index, short value );

    public abstract float getFloat( int index );
    public abstract RetainableByteBuffer putFloat( int index, float value );

    public abstract double getDouble( int index );
    public abstract RetainableByteBuffer putDouble( int index, double value );

    public final byte get()
    {
        return m_buf.get();
    }

    public final RetainableByteBuffer get( ByteBuffer dst )
    {
        dst.put( m_buf );
        return this;
    }

    public final RetainableByteBuffer get( byte [] dst )
    {
        return get( dst, 0, dst.length );
    }

    public final RetainableByteBuffer get( byte [] dst, int offset, int length )
    {
        m_buf.get( dst, offset, length );
        return this;
    }

    public final RetainableByteBuffer put( byte value )
    {
        m_buf.put( value );
        return this;
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

    public final RetainableByteBuffer put( byte [] src )
    {
        return put( src, 0, src.length );
    }

    public final RetainableByteBuffer put( byte [] src, int offset, int length )
    {
        m_buf.put( src, offset, length );
        return this;
    }

    public final RetainableByteBuffer putInt( int value )
    {
        m_buf.putInt( value );
        return this;
    }

    public final int getInt()
    {
        return m_buf.getInt();
    }

    public final RetainableByteBuffer putShort( short value )
    {
        m_buf.putShort( value );
        return this;
    }

    public final short getShort()
    {
        return m_buf.getShort();
    }

    public final RetainableByteBuffer putFloat( float value )
    {
        m_buf.putFloat( value );
        return this;
    }

    public final float getFloat()
    {
        return m_buf.getFloat();
    }

    public final RetainableByteBuffer putDouble( double value )
    {
        m_buf.putDouble( value );
        return this;
    }

    public final double getDouble()
    {
        return m_buf.getDouble();
    }

    public static RetainableByteBuffer allocate( int capacity )
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocate( capacity );
        return new Impl( byteBuffer );
    }

    public static RetainableByteBuffer allocateDirect( int capacity )
    {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect( capacity );
        return new Impl( byteBuffer );
    }
}
