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

/*
 * TimerQueue implementation use one thread from ThreadPool for timers wait,
 * and all timers are executed in the ThreadPool as well.
 * Another implementation specific is that cancel() call is synchronous,
 * i.e. TimerQueue guaranties that there is no thread executing timer task
 * on return from cancel() method.
 *
 * Time resolution is milliseconds, will be enough for most cases.
 */

package org.jsl.collider;

import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TimerQueue
{
    private static final Logger s_logger = Logger.getLogger( TimerQueue.class.getName() );

    private final ThreadPool m_threadPool;
    private final ReentrantLock m_lock;
    private final Condition m_cond;
    private final TreeMap<Long, TimerInfo> m_sortedTimers;
    private final Map<Runnable, TimerInfo> m_timers;
    private final ThreadPool.Runnable m_worker;

    private class TimerInfo extends ThreadPool.Runnable
    {
        public TimerInfo prev;
        public TimerInfo next;
        public final Runnable task;
        public long fireTime; /* milliseconds */
        public long period;   /* milliseconds */
        public final boolean dynamicRate;
        public long threadID;
        public int waiters;

        public TimerInfo( Runnable task, long fireTime, long period, boolean dynamicRate )
        {
            this.task = task;
            this.fireTime = fireTime;
            this.period = period;
            this.dynamicRate = dynamicRate;
        }

        public void runInThreadPool()
        {
            assert( threadID == -1 );
            assert( prev == null );
            assert( next == null );

            /* threadID value is important for this thread only during task.run() call,
             * can modify it here without lock.
             */
            threadID = Thread.currentThread().getId();

            task.run();

            m_lock.lock();
            try
            {
                threadID = 0;
                if (waiters > 0)
                {
                    m_timers.remove( task );
                    m_cond.signalAll();
                }
                else if (period > 0)
                {
                    final long currentTime = System.currentTimeMillis();
                    if (dynamicRate)
                        fireTime = (fireTime + (((currentTime - fireTime) / period) + 1) * period);
                    else
                        fireTime = (currentTime + period);

                    if (m_sortedTimers.isEmpty())
                    {
                        m_sortedTimers.put( fireTime, this );
                        m_threadPool.execute( m_worker );
                    }
                    else
                    {
                        next = m_sortedTimers.get( fireTime );
                        m_sortedTimers.put( fireTime, this );
                        if ((m_sortedTimers.firstKey() == fireTime) && (next == null))
                            m_cond.signalAll();
                    }
                }
                else
                {
                    m_timers.remove( task );
                    if (m_timers.isEmpty())
                        m_cond.signalAll();
                }
            }
            finally
            {
                m_lock.unlock();
            }
        }
    }

    private void run_i()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( "TimerQueue worker started" );

        m_lock.lock();
        try
        {
            while (!m_sortedTimers.isEmpty())
            {
                final Map.Entry<Long, TimerInfo> firstEntry = m_sortedTimers.firstEntry();
                assert( firstEntry != null );

                final long currentTime = System.currentTimeMillis();
                if (firstEntry.getKey() <= currentTime )
                {
                    TimerInfo timerInfo = firstEntry.getValue();
                    assert( timerInfo != null );
                    do
                    {
                        assert( timerInfo.threadID == 0 );
                        final TimerInfo next = timerInfo.next;
                        timerInfo.prev = null;
                        timerInfo.next = null;
                        /* Special value indicating the timer is being fired,
                         * will be changed to the proper value right before timer task execution.
                         */
                        timerInfo.threadID = -1;

                        m_threadPool.execute( timerInfo );
                        timerInfo = next;
                    }
                    while (timerInfo != null);
                    m_sortedTimers.remove( firstEntry.getKey() );
                }
                else
                {
                    final long sleepTime = (firstEntry.getKey() - currentTime);
                    try
                    {
                        m_cond.awaitNanos( TimeUnit.MILLISECONDS.toNanos(sleepTime) );
                    }
                    catch (InterruptedException ex)
                    {
                        s_logger.warning( ex.toString() );
                    }
                }
            }
        }
        finally
        {
            m_lock.unlock();
        }

        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( "TimerQueue worker finished" );
    }

    private void removeTimerLocked( TimerInfo timerInfo )
    {
        /* We will need to wake up worker thread only if timer
         * had the lowest firing time, and it was a one.
         */
        boolean wakeUpThread = false;

        if (timerInfo.prev == null)
        {
            if (timerInfo.next == null)
            {
                wakeUpThread = (m_sortedTimers.firstKey() == timerInfo.fireTime);
                m_sortedTimers.remove( timerInfo.fireTime );
            }
            else
            {
                m_sortedTimers.put( timerInfo.fireTime, timerInfo.next );
                timerInfo.next = null;
            }
        }
        else
        {
            timerInfo.prev = timerInfo.next;
            if (timerInfo.next != null)
            {
                timerInfo.next.prev = timerInfo.prev;
                timerInfo.next = null;
            }
            timerInfo.prev = null;
        }

        m_timers.remove( timerInfo.task );

        if (wakeUpThread)
            m_cond.signal();
    }

    private int schedule_i( Runnable task, long delay, long period, boolean dynamicRate )
    {
        m_lock.lock();
        try
        {
            if (m_timers.containsKey(task))
            {
                /* Timer already scheduled. */
                return -1;
            }

            boolean wasEmpty = m_sortedTimers.isEmpty();

            final long fireTime = (System.currentTimeMillis() + delay);
            final TimerInfo timerInfo = new TimerInfo( task, fireTime, period, dynamicRate );
            timerInfo.next = m_sortedTimers.get( fireTime );
            m_sortedTimers.put( fireTime, timerInfo );
            m_timers.put( task, timerInfo );

            if (!wasEmpty)
            {
                /* It make sense to wake up worker thread
                 * only if new timer is sooner than all previous.
                 */
                if (fireTime < m_sortedTimers.firstKey())
                    m_cond.signal();
                return 0;
            }
        }
        finally
        {
            m_lock.unlock();
        }

        /* Worker will be started only if timer queue was empty. */
        m_threadPool.execute( m_worker );
        return 0;
    }

    /**
     * Public methods
     */
    public TimerQueue( ThreadPool threadPool )
    {
        m_threadPool = threadPool;
        m_lock = new ReentrantLock();
        m_cond = m_lock.newCondition();
        m_sortedTimers = new TreeMap<Long, TimerInfo>();
        m_timers = new HashMap<Runnable, TimerInfo>();
        m_worker = new ThreadPool.Runnable() { public void runInThreadPool() { run_i(); } };
    }

    /**
     * Schedules the specified task for execution after the specified delay.
     */
    public final int schedule( Runnable task, long delay, TimeUnit unit )
    {
        return schedule_i( task, unit.toMillis(delay), 0, /*dynamic rate*/ false );
    }

    /**
     * Schedules the specified task for repeated fixed-rate execution,
     * beginning after the specified delay. Subsequent executions
     * take place at approximately regular intervals, separated by the specified period.
     * Working time line looks like:
     * +---------+-----------------+--------+------------------
     *    delay  |                 | period |
     *
     * +---------+-timer-work-time-+--------+------------------
     *                                       \
     *                                        next fire time
     */
    public final int scheduleAtFixedRate( Runnable task, long delay, long period, TimeUnit timeUnit )
    {
        return schedule_i( task, timeUnit.toMillis(delay), timeUnit.toMillis(period), /*dynamic rate*/ false );
    }

    /**
     * Schedules the specified task for repeated dynamic-rate execution,
     * beginning after the specified delay. Subsequent executions
     * take place at approximately regular intervals, separated by the specified period.
     * Working time line looks like:
     * +---------+----------+----------+------------------
     *    delay  |  period  |  period  |
     *
     * +---------+-timer-work-time-+---+------------------
     *                                  \
     *                                   next fire time
     * So timer will be executed exactly at a (delay + period*n) time,
     * skipping time if timer handler execution took too much time.
     */
    public final int scheduleAtDynamicRate( Runnable task, long delay, long period, TimeUnit timeUnit )
    {
        return schedule_i( task, timeUnit.toMillis(delay), timeUnit.toMillis(period), /*dynamic rate*/ true );
    }

    /**
     * Cancel timer,
     * waits if timer is being firing at the moment,
     * so it guarantees that timer handler is not executed
     * on return.
     */
    public final int cancel( Runnable task ) throws InterruptedException
    {
        m_lock.lock();
        try
        {
            final TimerInfo timerInfo = m_timers.get( task );
            if (timerInfo == null)
            {
                /* Timer already canceled or was not scheduled. */
                return -1;
            }

            if (timerInfo.threadID != 0)
            {
                /* Timer task is being executed now. */
                if (timerInfo.threadID == Thread.currentThread().getId())
                {
                    /* Called from the timer callback,
                     * let's just reset the period to avoid timer rescheduling.
                     */
                    timerInfo.period = 0;
                }
                else
                {
                    timerInfo.waiters++;
                    while (timerInfo.threadID != 0)
                        m_cond.await();
                    timerInfo.waiters--;
                }
                return 0;
            }

            removeTimerLocked( timerInfo );
        }
        finally
        {
            m_lock.unlock();
        }
        return 0;
    }

    /**
     * Cancel timer,
     * do not wait if runnable is being executed at that moment,
     * return > 0 in this case.
     */
    public final int cancelNoWait( Runnable task )
    {
        m_lock.lock();
        try
        {
            final TimerInfo timerInfo = m_timers.get( task );
            if (timerInfo == null)
            {
                /* Timer already canceled or was not scheduled. */
                return -1;
            }

            if (timerInfo.threadID != 0)
            {
                /* Timer task is being executed now. */
                return 1;
            }

            removeTimerLocked( timerInfo );
        }
        finally
        {
            m_lock.unlock();
        }
        return 0;
    }
}
