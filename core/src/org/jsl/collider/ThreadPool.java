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

package org.jsl.collider;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ThreadPool
{
    public static abstract class Task
    {
        public volatile Task nextThreadPoolTask;
        public abstract void run();
    }

    private class SvcThread extends Thread
    {
        public SvcThread( int id )
        {
            super( "CTPT-" + id );
        }

        public void run()
        {
            if (s_logger.isLoggable(Level.FINE))
                s_logger.fine( Thread.currentThread().getName() + ": started." );

            mainLoop: for (;;)
            {
                taskLoop: for (;;)
                {
                    Task head = m_head.get();
                    for (;;)
                    {
                        if (head == null)
                        {
                            if (m_stop)
                                break mainLoop;
                            else
                                break taskLoop;
                        }

                        Task next = head.nextThreadPoolTask;
                        if (m_head.compareAndSet(head, next))
                        {
                            if (next == null)
                            {
                                if (!m_tail.compareAndSet(head, null))
                                {
                                    while (head.nextThreadPoolTask == null);
                                    m_head.set( head.nextThreadPoolTask );
                                }
                            }
                            break;
                        }

                        head = m_head.get();
                    }

                    head.nextThreadPoolTask = null;
                    head.run();
                }

                m_idleThreads.incrementAndGet();

                try
                {
                    m_sem.acquire();
                }
                catch (InterruptedException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( Thread.currentThread().getName() + ": " + ex.toString() );
                    break;
                }
            }

            if (s_logger.isLoggable(Level.FINE))
                s_logger.fine( Thread.currentThread().getName() + ": finished." );
        }
    }

    private static final Logger s_logger = Logger.getLogger( ThreadPool.class.getName() );

    private Thread [] m_thread;
    private volatile boolean m_stop;
    private final Semaphore m_sem;
    private final AtomicInteger m_idleThreads;
    private final AtomicReference<Task> m_head;
    private final AtomicReference<Task> m_tail;

    public ThreadPool()
    {
        m_thread = null;
        m_stop = false;
        m_sem = new Semaphore(0);
        m_idleThreads = new AtomicInteger();
        m_head = new AtomicReference<Task>();
        m_tail = new AtomicReference<Task>();
    }

    public final void start( int threads )
    {
        assert( m_thread == null );
        m_thread = new Thread[threads];
        for (int idx=0; idx<threads; idx++)
        {
            Thread thread = new SvcThread( idx );
            m_thread[idx] = thread;
            thread.start();
        }
    }

    public final void stopAndWait() throws InterruptedException
    {
        assert( m_thread != null );

        m_stop = true;

        for (;;)
        {
            int idleThreads = m_idleThreads.get();
            if (idleThreads == 0)
                break;

            if (m_idleThreads.compareAndSet(idleThreads, 0))
            {
                m_sem.release( idleThreads );
                break;
            }
        }

        for (int idx=0; idx<m_thread.length; idx++)
        {
            if (m_thread[idx] != null)
            {
                m_thread[idx].join();
                m_thread[idx] = null;
            }
        }

        m_thread = null;
    }

    public void execute( Task task )
    {
        assert( task.nextThreadPoolTask == null );

        Task tail = m_tail.getAndSet( task );
        if (tail == null)
            m_head.set( task );
        else
            tail.nextThreadPoolTask = task;

        int idleThreads = m_idleThreads.get();
        for (;;)
        {
            if (idleThreads == 0)
                break;

            int newIdleThreads = (idleThreads - 1);
            if (m_idleThreads.compareAndSet(idleThreads, newIdleThreads))
            {
                m_sem.release();
                break;
            }

            idleThreads = m_idleThreads.get();
        }
    }
}
