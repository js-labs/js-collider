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

import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class ObjectCache<TYPE>
{
    private final String m_name;
    private final ReentrantLock m_lock;
    private final TYPE [] m_cache;
    private int m_size;
    private int m_gets;
    private int m_puts;
    private int m_miss;

    protected abstract TYPE allocateObject();

    public ObjectCache( String name, TYPE [] cache )
    {
        m_name = name;
        m_lock = new ReentrantLock();
        m_cache = cache;
    }

    public final boolean put( TYPE obj )
    {
        m_lock.lock();
        try
        {
            m_puts++;
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
        try
        {
            m_gets++;
            if (m_size > 0)
            {
                final int idx = --m_size;
                assert( m_cache[idx] != null );
                TYPE ret = m_cache[idx];
                m_cache[idx] = null;
                return ret;
            }
            m_miss++;
        }
        finally
        {
            m_lock.unlock();
        }
        return allocateObject();
    }

    public final void clear( Logger logger )
    {
        for (int idx=0; idx<m_size; idx++)
        {
            assert( m_cache[idx] != null );
            m_cache[idx] = null;
        }

        if (m_puts != m_gets)
        {
            if (logger.isLoggable(Level.WARNING))
            {
                logger.log( Level.WARNING,
                        m_name + ": resource leak detected: gets=" + m_gets + ", puts=" + m_puts + "." );
            }
        }
        else
        {
            if (logger.isLoggable(Level.FINE))
            {
                logger.log(
                        Level.FINE, m_name + ": size=" + m_size +
                        " (get=" + m_gets + ", miss=" + m_miss + ", put" + m_puts + ")." );
            }
        }

        m_size = 0;
    }

    public String clear( int initialSize )
    {
        for (int idx=0; idx<m_size; idx++)
        {
            assert( m_cache[idx] != null );
            m_cache[idx] = null;
        }

        final int size = m_size;
        m_size = 0;

        if (m_puts != m_gets)
        {
            return m_name + ": WARNING: resource leak detected: current size " +
                   size + " less than initial size " + initialSize +
                   " (" + m_gets + ", " + m_miss + ", " + m_puts + ").";
        }
        else
        {
            return m_name + ": size=" + size + " (get=" + m_gets +
                   ", miss=" + m_miss + ", put=" + m_puts + ").";
        }
    }
}
