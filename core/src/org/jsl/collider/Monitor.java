/*
 * JS-Collider framework.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.org
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.jsl.collider;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class Monitor
{
    private ReentrantLock m_lock;
    private Condition m_cond;
    private int m_count;

    public Monitor( int count )
    {
        m_lock = new ReentrantLock();
        m_cond = m_lock.newCondition();
        m_count = count;
    }

    public void acquire()
    {
        m_lock.lock();
        m_count++;
        m_lock.unlock();
    }

    public void release()
    {
        m_lock.lock();
        if (--m_count == 0)
            m_cond.signalAll();
        m_lock.unlock();
    }

    public void await()
    {
        m_lock.lock();
        try
        {
            while (m_count > 0)
                m_cond.await();
        }
        catch (InterruptedException ex)
        {
            System.out.println( ex.toString() );
        }
        m_lock.unlock();
    }
}
