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

package org.jsl.tests.thread_pool;

import org.jsl.collider.ThreadPool;
import org.jsl.collider.Collider;
import org.jsl.tests.Util;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;


public class Main
{
    private static final int POOL_THREADS = 4;
    private static final int PRODUCERS    = 4;
    private static int LAST_EVENT = 0;

    private int TEST_EVENTS = 1000000;
    private final ThreadPool m_threadPool;
    private final Semaphore m_semDone;
    private final AtomicInteger m_state;
    private long m_startTime;
    private long m_endTime;

    private class TestRunnable extends Collider.ThreadPoolRunnable
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
            else if (m_value == LAST_EVENT)
            {
                int state = m_state.incrementAndGet();
                if (state == (PRODUCERS*2))
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
            int events = (TEST_EVENTS / PRODUCERS);
            TestRunnable [] runnable = new TestRunnable[events];
            for (int idx=0; idx<events; idx++)
                runnable[idx] = new TestRunnable(idx);

            long startTime = System.nanoTime();
            for (int idx=0; idx<events; idx++)
                m_threadPool.execute( runnable[idx] );
            long endTime = System.nanoTime();

            System.out.println( events + " events scheduled at " +
                                Util.formatDelay(startTime, endTime) + " sec." );
        }
    }

    private Main()
    {
        TEST_EVENTS = (TEST_EVENTS / PRODUCERS) * PRODUCERS;
        LAST_EVENT = ((TEST_EVENTS / PRODUCERS) - 1);
        m_threadPool = new ThreadPool( "TP", POOL_THREADS );
        m_semDone = new Semaphore(0);
        m_state = new AtomicInteger(0);
    }

    public void run()
    {
        m_threadPool.start();

        Thread [] producer = new Thread[PRODUCERS];
        for (int idx=0; idx<PRODUCERS; idx++)
            (producer[idx] = new Thread(new Producer())).start();

        try
        {
            m_semDone.acquire();
            System.out.println( TEST_EVENTS + " events processed at " +
                                Util.formatDelay(m_startTime, m_endTime) + " sec. (" +
                                POOL_THREADS + " workers).");
            m_threadPool.stopAndWait();

            for (int idx=0; idx<PRODUCERS; idx++)
                producer[idx].join();
        }
        catch (InterruptedException ex)
        {
            System.out.println(ex);
        }
    }

    public static void main( String [] args )
    {
        new Main().run();
    }
}
