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

package org.jsl.tests.sched_latency;

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SL_Cond
{
    private static class Event
    {
        private final ReentrantLock m_lock;
        private final Condition m_cond;
        private boolean m_signal;

        public Event()
        {
            m_lock = new ReentrantLock();
            m_cond = m_lock.newCondition();
            m_signal = false;
        }

        public void signal()
        {
            m_lock.lock();
            try
            {
                m_signal = true;
                m_cond.signal();
            }
            finally
            {
                m_lock.unlock();
            }
        }

        public void await()
        {
            m_lock.lock();
            try
            {
                while (!m_signal)
                    m_cond.await();
                m_signal = false;
            }
            catch (InterruptedException ex)
            {
                ex.printStackTrace();
            }
            finally
            {
                m_lock.unlock();
            }
        }
    }

    private class TestThread implements Runnable
    {
        private final Event m_evt1;
        private final Event m_evt2;

        public TestThread( Event evt1, Event evt2 )
        {
            m_evt1 = evt1;
            m_evt2 = evt2;
        }

        public void run()
        {
            for (;;)
            {
                m_evt1.await();

                if (m_idx == m_res.length)
                    break;

                long time = System.nanoTime();
                m_res[m_idx] = (time - m_time);
                m_idx++;
                m_time = time;

                m_evt2.signal();
            }

            m_evt2.signal();
            m_semDone.release();
        }
    }

    private final long [] m_res;
    private final Semaphore m_semDone;
    private long m_time;
    private int m_idx;

    public SL_Cond( long [] res )
    {
        m_res = res;
        m_semDone = new Semaphore(0);
    }

    public void start()
    {
        Event evt1 = new Event();
        Event evt2 = new Event();

        new Thread(new TestThread(evt1, evt2)).start();
        new Thread(new TestThread(evt2, evt1)).start();

        m_time = System.nanoTime();
        evt1.signal();

        try { m_semDone.acquire(2); }
        catch (InterruptedException ex) { System.out.println(ex); }
    }
}
