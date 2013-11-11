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

import org.jsl.tests.Util;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExecutorTest extends Test
{
    private final AtomicInteger m_state;
    private final ExecutorService m_executor;
    private long m_startTime;
    private long m_endTime;

    private class TestRunnable implements Runnable
    {
        private int m_value;

        public TestRunnable( int value )
        {
            m_value = value;
        }

        public void run()
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
                    m_endTime = System.nanoTime();
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
                runnable[idx] = new TestRunnable( idx );
            runnable[events-1] = new TestRunnable( Integer.MIN_VALUE );

            long startTime = System.nanoTime();
            for (int idx=0; idx<events; idx++)
                m_executor.execute( runnable[idx] );
            long endTime = System.nanoTime();

            System.out.println( events + " events scheduled at " +
                    Util.formatDelay(startTime, endTime) + " sec." );
        }
    }

    public ExecutorTest( int totalEvents, int producers, int workers )
    {
        super( totalEvents, producers, workers );
        m_state = new AtomicInteger();
        m_executor = Executors.newFixedThreadPool( workers );
    }

    public String getName()
    {
        return "Executor";
    }

    public long runTest()
    {
        Thread [] producer = new Thread[m_producers];
        for (int idx=0; idx<m_producers; idx++)
            (producer[idx] = new Thread(new Producer())).start();

        try
        {
            for (int idx=0; idx<m_producers; idx++)
                producer[idx].join();

            m_executor.shutdown();
            if (!m_executor.awaitTermination(5, TimeUnit.SECONDS))
                m_executor.shutdownNow();
            m_executor.awaitTermination( 5, TimeUnit.SECONDS );
        }
        catch (InterruptedException ex)
        {
            System.out.println(ex);
        }

        return (m_endTime - m_startTime);
    }
}
