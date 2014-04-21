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

package org.jsl.collider;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.logging.Level;
import java.util.logging.Logger;


public class ThreadPool
{
    public static abstract class Runnable
    {
        public volatile Runnable nextThreadPoolRunnable;
        public abstract void runInThreadPool();
    }

    private static class Sync extends AbstractQueuedSynchronizer
    {
        private final int m_maxState;

        public Sync( int maxState )
        {
            m_maxState = maxState;
        }

        protected final int tryAcquireShared( int acquires )
        {
            for (;;)
            {
                int state = getState();
                int newState = (state - 1);
                if ((newState < 0) || compareAndSetState(state, newState))
                    return newState;
            }
        }

        protected final boolean tryReleaseShared( int releases )
        {
            for (;;)
            {
                int state = getState();
                if (state == m_maxState)
                    return false;
                if (compareAndSetState(state, state+releases))
                    return true;
            }
        }
    }

    private class Worker extends Thread
    {
        public void run()
        {
            final String name = m_name + "-" + getId();
            setName( name );

            if (s_logger.isLoggable(Level.FINE))
                s_logger.log( Level.FINE, name + ": started." );

            int idx = 0;
            while (m_run)
            {
                m_sync.acquireShared(1);

                int cc = m_contentionFactor;
                for (;;)
                {
                    Runnable runnable = getNext( idx );
                    if (runnable == null)
                    {
                        if (--cc == 0)
                            break;
                    }
                    else
                    {
                        runnable.runInThreadPool();
                        cc = m_contentionFactor;
                    }
                    idx++;
                    idx %= m_contentionFactor;
                }
            }

            if (s_logger.isLoggable(Level.FINE))
                s_logger.log( Level.FINE, name + ": finished." );
        }
    }

    private Runnable getNext( int idx )
    {
        idx = (idx * FS_PADDING) + FS_PADDING - 1;
        for (;;)
        {
            final Runnable runnable = m_hra.get( idx );

            if (runnable == null)
                return null;

            /* Schmidt D. algorithm suitable for the garbage collector
             * environment can not be used here because the same Runnable
             * can be scheduled multiple times (to avoid useless object allocation).
             */

            if (runnable == LOCK)
                continue;

            if (m_hra.compareAndSet(idx, runnable, LOCK))
            {
                if (runnable.nextThreadPoolRunnable == null)
                {
                    m_hra.set( idx, null );
                    if (!m_tra.compareAndSet(idx, runnable, null))
                    {
                        while (runnable.nextThreadPoolRunnable == null);
                        m_hra.set( idx, runnable.nextThreadPoolRunnable );
                        s_nextUpdater.lazySet( runnable, null );
                    }
                }
                else
                {
                    m_hra.set( idx, runnable.nextThreadPoolRunnable );
                    s_nextUpdater.lazySet( runnable, null );
                }
                return runnable;
            }
        }
    }

    private static class Lock extends Runnable
    {
        public void runInThreadPool()
        {
        }
    }

    private static final AtomicReferenceFieldUpdater<Runnable, Runnable> s_nextUpdater
            = AtomicReferenceFieldUpdater.newUpdater( Runnable.class, Runnable.class, "nextThreadPoolRunnable" );

    private static final Logger s_logger = Logger.getLogger( ThreadPool.class.getName() );
    private static final Lock LOCK = new Lock();
    private static final int FS_PADDING = 16;

    private final String m_name;
    private final int m_contentionFactor;
    private final Thread [] m_thread;
    private final Sync m_sync;
    private final AtomicReferenceArray<Runnable> m_hra;
    private final AtomicReferenceArray<Runnable> m_tra;
    private volatile boolean m_run;

    public ThreadPool( String name, int threads, int contentionFactor )
    {
        m_name = name;

        assert( contentionFactor >= 1 );
        if (contentionFactor < 1)
            contentionFactor = 1;

        m_contentionFactor = contentionFactor;

        m_thread = new Thread[threads];
        for (int idx=0; idx<threads; idx++)
            m_thread[idx] = new Worker();

        m_sync = new Sync( threads );
        m_hra = new AtomicReferenceArray<Runnable>( contentionFactor * FS_PADDING );
        m_tra = new AtomicReferenceArray<Runnable>( contentionFactor * FS_PADDING );
        m_run = true;
    }

    public ThreadPool( String name, int threads )
    {
        this( name, threads, 4 );
    }

    public final void start()
    {
        for (Thread thread : m_thread)
            thread.start();
    }

    public final void stopAndWait() throws InterruptedException
    {
        assert( m_thread != null );

        m_run = false;
        m_sync.releaseShared( m_thread.length );
        for (int idx=0; idx<m_thread.length; idx++)
        {
            m_thread[idx].join();
            m_thread[idx] = null;
        }
    }

    public final void execute( Runnable runnable )
    {
        assert( runnable.nextThreadPoolRunnable == null );

        int idx = (int) Thread.currentThread().getId();
        idx = (idx % m_contentionFactor) * FS_PADDING + FS_PADDING - 1;

        final Runnable tail = m_tra.getAndSet( idx, runnable );
        if (tail == null)
            m_hra.set( idx, runnable );
        else
            tail.nextThreadPoolRunnable = runnable;

        m_sync.releaseShared(1);
    }
}
