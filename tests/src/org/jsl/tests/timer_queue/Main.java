/*
 * JS-Collider framework tests.
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

package org.jsl.tests.timer_queue;

import org.jsl.collider.ThreadPool;
import org.jsl.collider.TimerQueue;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main
{
    private static final long DIFF_THRESHOLD = 30;
    private final AtomicInteger m_done;
    private final Semaphore m_sema;

    private class Timer1 implements TimerQueue.Task
    {
        private final TimerQueue m_timerQueue;

        public Timer1( TimerQueue timerQueue )
        {
            m_timerQueue = timerQueue;
        }

        public long run()
        {
            try
            {
                int rc = m_timerQueue.cancel( this );
                assert( rc == -1 );
                System.out.println( "Test1 [callback cancel] done." );
            }
            catch (final InterruptedException ex)
            {
                ex.printStackTrace();
            }

            final int done = m_done.decrementAndGet();
            if (done == 0)
                m_sema.release();

            return 0;
        }
    }

    private class Timer2 implements TimerQueue.Task
    {
        private final long m_interval;
        private long m_lastFireTime;
        private int m_cnt;

        public Timer2( long interval )
        {
            m_interval = interval;
        }

        public long run()
        {
            /* Execution time should be first time + (cnt * period) */
            final long currentTime = System.currentTimeMillis();
            final int cnt = m_cnt++;

            if (m_lastFireTime == 0)
                System.out.println( "Timer2: first fire" );
            else
            {
                final long diff = (currentTime - m_lastFireTime - m_interval);
                System.out.println( "Timer2: diff=" + diff );
                if (diff > DIFF_THRESHOLD)
                    throw new RuntimeException( "Timer2: missed too much: " + diff );
            }
            m_lastFireTime = currentTime;

            if (cnt == 5)
            {
                final int done = m_done.decrementAndGet();
                if (done == 0)
                    m_sema.release();
                return 0;
            }
            else
                return m_interval;
        }
    }

    private class Timer3 implements TimerQueue.Task
    {
        private final long m_interval;
        private long m_lastFireTime;
        private int m_cnt;

        public Timer3( long interval )
        {
            m_interval = interval;
        }

        public long run()
        {
            /* Execution time should be first time + (cnt * period) */
            final long currentTime = System.currentTimeMillis();
            final int cnt = m_cnt++;

            if (m_lastFireTime == 0)
                System.out.println( "Timer3: first fire" );
            else
            {
                final long diff = (currentTime - m_lastFireTime - m_interval);
                System.out.println( "Timer3: diff=" + diff );
                if (diff > DIFF_THRESHOLD)
                    throw new RuntimeException( "Timer3: missed too much: " + diff );
            }
            m_lastFireTime = currentTime;

            if (cnt == 8)
            {
                final int done = m_done.decrementAndGet();
                if (done == 0)
                    m_sema.release();
                return 0;
            }
            else
                return m_interval;
        }
    }

    private static class Timer4 implements TimerQueue.Task
    {
        public long run()
        {
            throw new RuntimeException( "Method should never be called" );
        }
    }

    private Main()
    {
        m_done = new AtomicInteger();
        m_sema = new Semaphore(0);
    }

    private void run()
    {
        m_done.set(3); /* 3 tests */

        final ThreadPool threadPool = new ThreadPool( "TP", 4 );
        threadPool.start();

        final TimerQueue timerQueue = new TimerQueue( threadPool );

        final Timer1 timer1 = new Timer1( timerQueue );
        timerQueue.schedule( timer1, 100, TimeUnit.MILLISECONDS );

        final long test2Interval = 500;
        final Timer2 timer2 = new Timer2( test2Interval );
        timerQueue.schedule( timer2, 100, TimeUnit.MILLISECONDS );

        final long test3Interval = test2Interval;
        final Timer3 timer3 = new Timer3( test3Interval );
        timerQueue.schedule( timer3, 100, TimeUnit.MILLISECONDS );

        final Timer4 timer4 = new Timer4();
        timerQueue.schedule( timer4, 10, TimeUnit.SECONDS );

        try
        {
            int rc = timerQueue.cancel( timer4 );
            if (rc != 0)
                throw new RuntimeException( "Timer not canceled!" );

            m_sema.acquire();
            threadPool.stopAndWait();
        }
        catch (final InterruptedException ex)
        {
            ex.printStackTrace();
        }
    }

    public static void main( String [] args )
    {
        new Main().run();
    }
}
