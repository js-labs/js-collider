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

package org.jsl.tests.thread_pool_throughput;
import org.jsl.collider.ThreadPool;
import org.jsl.tests.Util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolTest extends Test
{
    private final ThreadPool m_threadPool;
    private final Semaphore m_semReady;
    private final Semaphore m_semStart;
    private final Semaphore m_semDone;
    private final AtomicInteger m_state;
    private long m_startTime;
    private long m_endTime;

    private class TestRunnable extends ThreadPool.Runnable
    {
        private int m_value;

        public TestRunnable( int value )
        {
            m_value = value;
        }

        public void runInThreadPool()
        {
            if (m_value == 0)
            {
                int state = m_state.incrementAndGet();
                if (state == 1)
                    m_startTime = System.nanoTime();
            }
            else if (m_value == Integer.MIN_VALUE)
            {
                int state = m_state.incrementAndGet();
                if (state == (m_producers*2))
                {
                    m_endTime = System.nanoTime();
                    m_semDone.release();
                }
            }
        }
    }

    private class Producer implements Runnable
    {
        public void run()
        {
            int events = (m_totalEvents / m_producers);
            TestRunnable [] runnable = new TestRunnable[events];
            for (int idx=0; idx<events-1; idx++)
                runnable[idx] = new TestRunnable(idx);
            runnable[events-1] = new TestRunnable( Integer.MIN_VALUE );

            m_semReady.release();
            try { m_semStart.acquire(); }
            catch (InterruptedException ignored) {}

            long startTime = System.nanoTime();
            for (int idx=0; idx<events; idx++)
                m_threadPool.execute( runnable[idx] );
            long endTime = System.nanoTime();

            System.out.println( events + " events scheduled at " +
                    Util.formatDelay(startTime, endTime) + " sec." );
        }
    }

    public ThreadPoolTest( int totalEvents, int producers, int workers )
    {
        super( totalEvents, producers, workers );
        m_threadPool = new ThreadPool( "TTP", workers );

        m_semReady = new Semaphore(0);
        m_semStart = new Semaphore(0);
        m_semDone = new Semaphore(0);
        m_state = new AtomicInteger();
    }

    public String getName()
    {
        return "ThreadPool";
    }

    public long runTest()
    {
        m_threadPool.start();

        Thread [] producer = new Thread[m_producers];
        for (int idx=0; idx<m_producers; idx++)
            (producer[idx] = new Thread(new Producer())).start();

        try
        {
            for (int idx=0; idx<m_producers; idx++)
                m_semReady.acquire();
            m_semStart.release( m_producers );
            m_semDone.acquire();
            m_threadPool.stopAndWait();

            for (int idx=0; idx<m_producers; idx++)
                producer[idx].join();
        }
        catch (InterruptedException ex)
        {
            System.out.println(ex);
        }

        return (m_endTime - m_startTime);
    }
}
