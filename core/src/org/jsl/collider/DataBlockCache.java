/*
 * JS-Collider framework.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.org
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

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.atomic.AtomicInteger;

public class DataBlockCache
{
    private final boolean m_useDirectBuffers;
    private final int m_blockSize;
    private final int m_maxSize;
    private final AtomicInteger m_state;
    private DataBlock m_dataBlock;
    private int m_size;

    public DataBlockCache( boolean useDirectBuffers, int blockSize, int initialSize, int maxSize )
    {
        m_useDirectBuffers = useDirectBuffers;
        m_blockSize = blockSize;
        m_maxSize = maxSize;
        m_state = new AtomicInteger();
        m_dataBlock = null;
        m_size = initialSize;

        for (int idx=0; idx<initialSize; idx++)
        {
            DataBlock dataBlock = new DataBlock( useDirectBuffers, blockSize );
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
        for (;;)
        {
            if (m_state.compareAndSet(0, 1))
                break;
        }

        try
        {
            if (m_size < m_maxSize)
            {
                DataBlock head = dataBlock;
                for (;;)
                {
                    if (++m_size == m_maxSize)
                        break;
                    if (dataBlock.next == null)
                        break;
                    dataBlock = dataBlock.next;
                }
                DataBlock next = dataBlock.next;
                dataBlock.next = m_dataBlock;
                m_dataBlock = head;
                dataBlock = next;
            }
        }
        finally
        {
            m_state.set(0);
        }

        while (dataBlock != null)
        {
            DataBlock next = dataBlock.next;
            dataBlock.next = null;
            dataBlock = next;
        }
    }

    public final DataBlock get( int cnt )
    {
        if (cnt <= 0)
        {
            assert( false );
            return null;
        }

        DataBlock ret = null;
        DataBlock dataBlock = null;

        for (;;)
        {
            if (m_state.compareAndSet(0, 1))
                break;
        }

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
            m_state.set(0);
        }

        if (ret == null)
        {
            ret = new DataBlock( m_useDirectBuffers, m_blockSize );
            if (--cnt == 0)
                return ret;
            dataBlock = ret;
        }

        for (; cnt>0; cnt--)
        {
            dataBlock.next = new DataBlock( m_useDirectBuffers, m_blockSize );
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

    public final void clear( Logger logger, int initialSize )
    {
        int size = 0;
        while (m_dataBlock != null)
        {
            DataBlock next = m_dataBlock.next;
            m_dataBlock.next = null;
            m_dataBlock = next;
            size++;
        }

        if (size != m_size)
        {
            if (logger.isLoggable(Level.WARNING))
            {
                logger.warning(
                        "[" + m_blockSize + "] internal error: real size " +
                        size + " != " + m_size + "." );
            }
        }

        if (size < initialSize)
        {
            if (logger.isLoggable(Level.WARNING))
            {
                logger.warning(
                        "[" + m_blockSize + "] resource leak detected: current size " +
                         size + " less than initial size (" + initialSize + ")." );
            }
        }
        else if (size > m_maxSize)
        {
            if (logger.isLoggable(Level.WARNING))
            {
                logger.warning(
                        "[" + m_blockSize + "] internal error: current size " +
                         size + " is greater than maximum size (" + m_maxSize + ")." );
            }
        }
        else
        {
            if (logger.isLoggable(Level.FINE))
                logger.fine( "[" + m_blockSize + "] size=" + size + "." );
        }
    }
}
