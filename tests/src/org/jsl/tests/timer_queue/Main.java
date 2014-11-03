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
    private final AtomicInteger m_done;
    private final Semaphore m_sema;

    private class Timer1 implements Runnable
    {
        private final TimerQueue m_timerQueue;

        public Timer1( TimerQueue timerQueue )
        {
            m_timerQueue = timerQueue;
        }

        public void run()
        {
            try
            {
                int rc = m_timerQueue.cancel( this );
                assert( rc == 0 );
                System.out.println( "Test1 [callback cancel] done." );
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }

            final int done = m_done.decrementAndGet();
            if (done == 0)
                m_sema.release();
        }
    }

    private class Timer2 implements Runnable
    {
        private final TimerQueue m_timerQueue;
        private final long m_period;
        private long m_firstFireTime;
        private int m_cnt;

        public Timer2( TimerQueue timerQueue, long period )
        {
            m_timerQueue = timerQueue;
            m_period = period;
        }

        public void run()
        {
            /* Execution time should be first time + (cnt * period) */
            final long currentTime = System.currentTimeMillis();
            final int cnt = m_cnt++;

            if (cnt == 0)
                m_firstFireTime = currentTime;
            else
            {
                final long diff = (currentTime - m_firstFireTime) - (cnt * m_period);
                System.out.println(
                        "Timer2: " + (currentTime - m_firstFireTime) + " diff=" + diff );
                if (diff > 100)
                    throw new RuntimeException( "Missed too much: " + diff );
            }

            if (cnt == 5)
            {
                try
                {
                    int rc = m_timerQueue.cancel( this );
                    if (rc != 0)
                        throw new RuntimeException( "TimerQueue.cancel() failed." );
                }
                catch (InterruptedException ex)
                {
                    ex.printStackTrace();
                }

                final int done = m_done.decrementAndGet();
                if (done == 0)
                    m_sema.release();
            }
        }
    }

    private class Timer3 implements Runnable
    {
        private final TimerQueue m_timerQueue;
        private final long m_period;
        private final long m_sleepTime;
        private long m_lastTime;
        private int m_cnt;

        public Timer3( TimerQueue timerQueue, long period, long sleepTime )
        {
            m_timerQueue = timerQueue;
            m_period = period;
            m_sleepTime = sleepTime;
        }

        public void run()
        {
            final long currentTime = System.currentTimeMillis();
            final int cnt = m_cnt++;

            if (cnt > 0)
            {
                final double cc = ((double)(currentTime-m_lastTime)) / m_period;
                final double diff = Math.abs( cc - Math.rint(cc) );
                System.out.println( "Timer3: " + (currentTime-m_lastTime) + ", " + cc + ", " + diff );
                if ((cnt > 1) && (diff > 0.5d))
                    throw new RuntimeException( "Missed too much!" );
            }
            m_lastTime = currentTime;

            if (cnt == 7)
            {
                final int done = m_done.decrementAndGet();
                if (done == 0)
                    m_sema.release();

                try
                {
                    int rc = m_timerQueue.cancel( this );
                    if (rc != 0)
                        throw new RuntimeException( "TimerQueue.cancel() failed." );
                }
                catch (InterruptedException ex)
                {
                    ex.printStackTrace();
                }
            }

            try { Thread.sleep( m_sleepTime ); }
            catch (InterruptedException ex) { ex.printStackTrace(); }
        }
    }

    private static class Timer4 implements Runnable
    {
        public void run()
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
        timerQueue.schedule( timer1, 100, TimeUnit.MICROSECONDS );

        final long test2Period = 500;
        final Timer2 timer2 = new Timer2( timerQueue, test2Period );
        timerQueue.scheduleAtFixedRate( timer2, 100, test2Period, TimeUnit.MICROSECONDS );

        /* 110 220 330 440 550 660 770 880 990
         *          +           +           +
         */
        final long test3Period = 110;
        final Timer3 timer3 = new Timer3( timerQueue, test3Period, /*sleep time*/ 300 );
        timerQueue.scheduleAtDynamicRate( timer3, 0, test3Period, TimeUnit.MICROSECONDS );

        final Timer4 timer4 = new Timer4();
        timerQueue.schedule( timer4, 300, TimeUnit.MICROSECONDS );

        try
        {
            int rc = timerQueue.cancel( timer4 );
            if (rc != 0)
                throw new RuntimeException( "Timer not canceled!" );

            m_sema.acquire();
            threadPool.stopAndWait();
        }
        catch (InterruptedException ex)
        {
            ex.printStackTrace();
        }
    }

    public static void main( String [] args )
    {
        new Main().run();
    }
}
