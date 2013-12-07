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

import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class ObjectCache<TYPE>
{
    private final ReentrantLock m_lock;
    private final TYPE [] m_cache;
    private int m_size;

    protected abstract TYPE allocateObject();

    public ObjectCache( TYPE [] cache )
    {
        m_lock = new ReentrantLock();
        m_cache = cache;
        m_size = 0;
    }

    public final boolean put( TYPE obj )
    {
        m_lock.lock();
        try
        {
            if (m_size < m_cache.length)
            {
                final int idx = m_size++;
                assert( m_cache[idx] == null );
                m_cache[idx] = obj;
                return true;
            }
        }
        finally
        {
            m_lock.unlock();
        }
        return false;
    }

    public final TYPE get()
    {
        m_lock.lock();
        if (m_size > 0)
        {
            try
            {
                final int idx = --m_size;
                assert( m_cache[idx] != null );
                TYPE ret = m_cache[idx];
                m_cache[idx] = null;
                return ret;
            }
            finally
            {
                m_lock.unlock();
            }
        }
        else
        {
            m_lock.unlock();
            return allocateObject();
        }
    }

    protected final String clear( int initialSize )
    {
        for (int idx=0; idx<m_size; idx++)
        {
            assert( m_cache[idx] != null );
            m_cache[idx] = null;
        }

        final int size = m_size;
        m_size = 0;

        if (m_size < initialSize)
        {
            return "resource leak detected: current size " +
                    size + " less than initial size (" + initialSize + ").";
        }
        else
        {
            return "size=" + size;
        }
    }
}
