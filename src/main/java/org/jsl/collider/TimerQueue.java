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

public class TimerQueue extends ThreadPool.Runnable
{
    private static final Logger s_logger = Logger.getLogger( TimerQueue.class.getName() );

    public interface Task
    {
        /* Method should return the time interval (in milliseconds)
         * the timer wish to fire next time.
         * Return 0 to cancel timer.
         */
        long run();
    }

    private final ThreadPool m_threadPool;
    private final ReentrantLock m_lock;
    private final Condition m_cond;
    private final TreeMap<Long, TimerInfo> m_sortedTimers;
    private final Map<Task, TimerInfo> m_timers;

    private class TimerInfo extends ThreadPool.Runnable
    {
        public final Task task;
        public TimerInfo prev;
        public TimerInfo next;
        public long fireTime;
        public long threadID;
        public Condition cond;

        public TimerInfo( Task task )
        {
            this.task = task;
        }

        public void runInThreadPool()
        {
            assert( threadID == -1 );
            assert( prev == null );
            assert( next == null );
            threadID = Thread.currentThread().getId();
            final long interval = task.run();
            restateTimer( this, interval );
        }
    }

    private void restateTimer( TimerInfo timerInfo, long interval )
    {
        boolean snatchThread = false;

        m_lock.lock();
        try
        {
            if (timerInfo.cond != null)
            {
                if (s_logger.isLoggable(Level.FINER))
                    s_logger.log( Level.FINER, System.identityHashCode(timerInfo.task) + ": pending cancel" );

                timerInfo.threadID = -2;
                timerInfo.cond.signalAll();
            }
            else if (interval > 0)
            {
                final long fireTime = (System.currentTimeMillis() + interval);
                timerInfo.threadID = 0;
                timerInfo.fireTime = fireTime;
                if (m_sortedTimers.isEmpty())
                {
                    if (s_logger.isLoggable(Level.FINER))
                    {
                        s_logger.log( Level.FINER, System.identityHashCode(timerInfo.task) +
                                ": interval=" + interval + ", snatch thread" );
                    }
                    m_sortedTimers.put( fireTime, timerInfo );
                    snatchThread = true;
                }
                else
                {
                    final TimerInfo next = m_sortedTimers.get( fireTime );
                    m_sortedTimers.put( fireTime, timerInfo );
                    if (next == null)
                    {
                        if (s_logger.isLoggable(Level.FINER))
                        {
                            s_logger.log( Level.FINER, System.identityHashCode(timerInfo.task) +
                                    ": interval=" + interval + ", wakeup thread" );
                        }
                        if (m_sortedTimers.firstKey() == fireTime)
                            m_cond.signal();
                    }
                    else
                    {
                        if (s_logger.isLoggable(Level.FINER))
                        {
                            s_logger.log( Level.FINER, System.identityHashCode(timerInfo.task) +
                                    ": interval=" + interval );
                        }
                        timerInfo.next = next;
                        next.prev = timerInfo;
                    }
                }
            }
            else
            {
                if (s_logger.isLoggable(Level.FINER))
                    s_logger.log( Level.FINER, System.identityHashCode(timerInfo.task) + ": done" );
                m_timers.remove( timerInfo.task );
            }
        }
        finally
        {
            m_lock.unlock();
        }

        if (snatchThread)
            runInThreadPool();
    }

    public void runInThreadPool()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.log( Level.FINE, "started" );

        m_lock.lock();
        try
        {
            while (!m_sortedTimers.isEmpty())
            {
                final Map.Entry<Long, TimerInfo> firstEntry = m_sortedTimers.firstEntry();
                assert( firstEntry != null );

                final long currentTime = System.currentTimeMillis();
                if (firstEntry.getKey() <= currentTime)
                {
                    if (s_logger.isLoggable(Level.FINER))
                        s_logger.log( Level.FINER, "fireTime=" + firstEntry.getKey() + ": execute" );

                    TimerInfo timerInfo = firstEntry.getValue();
                    do
                    {
                        assert( timerInfo.threadID == 0 );
                        final TimerInfo next = timerInfo.next;
                        timerInfo.prev = null;
                        timerInfo.next = null;
                        timerInfo.threadID = -1; /* timer is being fired */

                        m_threadPool.execute( timerInfo );
                        timerInfo = next;
                    }
                    while (timerInfo != null);
                    m_sortedTimers.remove( firstEntry.getKey() );
                }
                else
                {
                    final long sleepTime = (firstEntry.getKey() - currentTime);
                    if (s_logger.isLoggable(Level.FINER))
                        s_logger.log( Level.FINER, "firstEntry=" + firstEntry.getKey() + ", sleepTime=" + sleepTime );

                    try
                    {
                        m_cond.awaitNanos( TimeUnit.MILLISECONDS.toNanos(sleepTime) );
                    }
                    catch (final InterruptedException ex)
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
            s_logger.log( Level.FINE, "finished" );
    }

    private void removeTimerLocked( TimerInfo timerInfo )
    {
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
            timerInfo.prev.next = timerInfo.next;
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

    /**
     * Public methods
     */
    public TimerQueue( ThreadPool threadPool )
    {
        m_threadPool = threadPool;
        m_lock = new ReentrantLock();
        m_cond = m_lock.newCondition();
        m_sortedTimers = new TreeMap<Long, TimerInfo>();
        m_timers = new HashMap<Task, TimerInfo>();
    }

    /**
     * Schedules the specified task for execution after the specified delay.
     */
    public int schedule( Task task, long delay, TimeUnit unit )
    {
        m_lock.lock();
        try
        {
            if (m_timers.containsKey(task))
            {
                /* Timer already scheduled. */
                return -1;
            }

            final boolean wasEmpty = m_sortedTimers.isEmpty();
            final long fireTime = (System.currentTimeMillis() + unit.toMillis(delay));
            final TimerInfo timerInfo = new TimerInfo( task );
            timerInfo.fireTime = fireTime;

            final TimerInfo next = m_sortedTimers.get( fireTime );
            timerInfo.next = next;
            if (next != null)
                next.prev = timerInfo;

            m_sortedTimers.put( fireTime, timerInfo );
            m_timers.put( task, timerInfo );

            if (wasEmpty)
            {
                if (s_logger.isLoggable(Level.FINER))
                    s_logger.log( Level.FINER, System.identityHashCode(task) + ": fireTime=" + fireTime + ", start worker" );
                m_threadPool.execute( this );
            }
            else
            {
                /* It make sense to wake up worker thread
                 * only if new timer is sooner than all previous.
                 */
                if (fireTime < m_sortedTimers.firstKey())
                {
                    if (s_logger.isLoggable(Level.FINER))
                        s_logger.log( Level.FINER, System.identityHashCode(task) + ": firerTime=" + fireTime + ", wakeup worker" );
                    m_cond.signal();
                }
                else
                {
                    if (s_logger.isLoggable(Level.FINER))
                        s_logger.log( Level.FINER, System.identityHashCode(task) + ": fireTime=" + fireTime );
                }
            }
        }
        finally
        {
            m_lock.unlock();
        }
        return 0;
    }

    /**
     * Cancel timer,
     * waits if timer is being firing at the moment,
     * so it guarantees that timer handler is not executed on return.
     */
    public int cancel( Task task ) throws InterruptedException
    {
        m_lock.lock();
        try
        {
            for (;;)
            {
                final TimerInfo timerInfo = m_timers.get( task );
                if (timerInfo == null)
                {
                    /* Timer already canceled or was not scheduled. */
                    if (s_logger.isLoggable( Level.FINER))
                        s_logger.log( Level.FINER, System.identityHashCode(task) + ": not registered" );
                    return -1;
                }

                assert( timerInfo.task == task );

                if (timerInfo.threadID == Thread.currentThread().getId())
                {
                    /* Cancel from the timer callback */
                    return -1;
                }
                else if (timerInfo.threadID == 0)
                {
                    /* Timer is not fired yet */
                    if (s_logger.isLoggable( Level.FINER))
                        s_logger.log( Level.FINER, System.identityHashCode(task) + ": canceled" );
                    removeTimerLocked( timerInfo );
                    return 0;
                }
                else if (timerInfo.threadID == -2)
                {
                    /* Timer just fired */
                    if (s_logger.isLoggable(Level.FINER))
                        s_logger.log( Level.FINER, System.identityHashCode(task) + ": canceled, just fired" );
                    assert( timerInfo.cond != null );
                    timerInfo.cond = null;
                    m_timers.remove( task );
                    return 0;
                }
                else
                {
                    /* Timer is being executed now, let's wait */
                    Condition cond = timerInfo.cond;
                    if (cond == null)
                    {
                        cond = m_lock.newCondition();
                        timerInfo.cond = cond;
                    }
                    cond.await();
                }
            }
        }
        finally
        {
            m_lock.unlock();
        }
    }

    /**
     * Cancel timer,
     * do not wait if the task is being executed at that moment,
     * return > 0 in this case.
     */
    public int cancelNoWait( Task task )
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
