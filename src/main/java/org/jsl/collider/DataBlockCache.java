/*
 * Copyright (C) 2015 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of JS-Collider framework tests.
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
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DataBlockCache
{
    private final boolean m_useDirectBuffers;
    private final int m_blockSize;
    private final int m_initialSize;
    private final int m_maxSize;
    private final ReentrantLock m_lock;
    private DataBlock m_dataBlock;
    private int m_size;

    private DataBlock createDataBlock()
    {
        final ByteBuffer byteBuffer =
                m_useDirectBuffers
                    ? ByteBuffer.allocateDirect( m_blockSize )
                    : ByteBuffer.allocate( m_blockSize );
        return new DataBlock( byteBuffer );
    }

    public DataBlockCache( boolean useDirectBuffers, int blockSize, int initialSize, int maxSize )
    {
        m_useDirectBuffers = useDirectBuffers;
        m_blockSize = blockSize;
        m_initialSize = initialSize;
        m_maxSize = maxSize;
        m_lock = new ReentrantLock();
        m_dataBlock = null;
        m_size = initialSize;

        for (int idx=0; idx<initialSize; idx++)
        {
            DataBlock dataBlock = createDataBlock();
            dataBlock.next = m_dataBlock;
            m_dataBlock = dataBlock;
        }
    }

    public final int getBlockSize()
    {
        return m_blockSize;
    }

    public final void put( DataBlock dataBlock )
    {
        m_lock.lock();
        try
        {
            if (m_size < m_maxSize)
            {
                DataBlock head = dataBlock;
                for (;;)
                {
                    assert( dataBlock.rw.position() == 0 );
                    assert( dataBlock.ww.position() == 0 );
                    if (++m_size == m_maxSize)
                        break;
                    if (dataBlock.next == null)
                        break;
                    dataBlock = dataBlock.next;
                }
                final DataBlock next = dataBlock.next;
                dataBlock.next = m_dataBlock;
                m_dataBlock = head;
                dataBlock = next;
            }
        }
        finally
        {
            m_lock.unlock();
        }

        while (dataBlock != null)
        {
            final DataBlock next = dataBlock.next;
            dataBlock.next = null;
            dataBlock = next;
        }
    }

    public final DataBlock get( int cnt )
    {
        if (cnt <= 0)
            throw new AssertionError();

        DataBlock ret = null;
        DataBlock dataBlock = null;

        m_lock.lock();
        try
        {
            if (m_dataBlock != null)
            {
                ret = m_dataBlock;
                dataBlock = m_dataBlock;
                for (;;)
                {
                    m_size--;
                    if (--cnt == 0)
                    {
                        m_dataBlock = dataBlock.next;
                        dataBlock.next = null;
                        return ret;
                    }
                    if (dataBlock.next == null)
                        break;
                    dataBlock = dataBlock.next;
                }
                m_dataBlock = null;
            }
        }
        finally
        {
            m_lock.unlock();
        }

        if (ret == null)
        {
            ret = createDataBlock();
            if (--cnt == 0)
                return ret;
            dataBlock = ret;
        }

        for (; cnt>0; cnt--)
        {
            dataBlock.next = createDataBlock();
            dataBlock = dataBlock.next;
        }

        return ret;
    }

    public final DataBlock getByDataSize( int dataSize )
    {
        int blocks = (dataSize / m_blockSize);
        if ((dataSize % m_blockSize) > 0)
            blocks++;
        return get( blocks );
    }

    public final void clear( Logger logger )
    {
        int size = 0;
        while (m_dataBlock != null)
        {
            final DataBlock next = m_dataBlock.next;
            m_dataBlock.next = null;
            m_dataBlock = next;
            size++;
        }

        if (size != m_size)
        {
            if (logger.isLoggable(Level.WARNING))
            {
                logger.warning(
                        getClass().getSimpleName() +
                        "[" + m_blockSize + "] internal error: real size " +
                        size + " != " + m_size + "." );
            }
        }

        if (size < m_initialSize)
        {
            if (logger.isLoggable(Level.WARNING))
            {
                logger.warning(
                        getClass().getSimpleName() +
                        "[" + m_blockSize + "] resource leak detected: current size " +
                        size + " less than initial size (" + m_initialSize + ")." );
            }
        }
        else if (size > m_maxSize)
        {
            if (logger.isLoggable(Level.WARNING))
            {
                logger.warning(
                        getClass().getSimpleName() +
                        "[" + m_blockSize + "] internal error: current size " +
                        size + " is greater than maximum size (" + m_maxSize + ")." );
            }
        }
        else
        {
            if (logger.isLoggable(Level.FINE))
            {
                logger.fine(
                        getClass().getSimpleName() +
                        "[" + m_blockSize + "] size=" + size + "." );
            }
        }
    }
}
