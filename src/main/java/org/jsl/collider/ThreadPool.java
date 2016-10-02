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

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ThreadPool
{
    public static abstract class Runnable
    {
        public volatile Runnable nextThreadPoolRunnable;
        public abstract void runInThreadPool();
    }

    private class Worker extends Thread
    {
        private final int m_id;

        public Worker( int id )
        {
            m_id = id;
        }

        public void run()
        {
            final String name = m_name + "-" + getId();
            final int workerId = (1 << m_id);
            setName( name );

            if (s_logger.isLoggable(Level.FINE))
                s_logger.log( Level.FINE, name + ": started." );

            int parks = 0;
            int idx = 0;
            loop: for (;;)
            {
                int cc = m_contentionFactor;
                for (;;)
                {
                    final Runnable runnable = getNext( idx );
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

                for (;;)
                {
                    final int state = m_state;
                    if ((state & STATE_STOP) == 0)
                    {
                        if (state == STATE_SPIN)
                        {
                            if (s_stateUpdater.compareAndSet(ThreadPool.this, STATE_SPIN, 0))
                            {
                                /* At least one runnable was dispatched while all threads were not idle.
                                 * We have a possible race here, would be nice to check queues one more time.
                                 */
                                break;
                            }
                        }
                        else
                        {
                            if (s_stateUpdater.compareAndSet(ThreadPool.this, state, state|workerId))
                            {
                                parks++;
                                LockSupport.park();
                                break;
                            }
                        }
                    }
                    else
                        break loop;
                }
            }

            if (s_logger.isLoggable(Level.FINE))
                s_logger.log( Level.FINE, name + ": finished (" + parks + " parks)." );
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
                    if (m_tra.compareAndSet(idx, runnable, null))
                        return runnable;
                    while (runnable.nextThreadPoolRunnable == null);
                }
                m_hra.set( idx, runnable.nextThreadPoolRunnable );
                s_nextUpdater.lazySet( runnable, null );
                return runnable;
            }
        }
    }

    private static class DummyRunnable extends Runnable
    {
        public void runInThreadPool()
        {
            /* Should never be called */
            assert( false );
        }
    }

    private static final AtomicReferenceFieldUpdater<Runnable, Runnable> s_nextUpdater
            = AtomicReferenceFieldUpdater.newUpdater( Runnable.class, Runnable.class, "nextThreadPoolRunnable" );

    private static final AtomicIntegerFieldUpdater<ThreadPool> s_stateUpdater
            = AtomicIntegerFieldUpdater.newUpdater( ThreadPool.class, "m_state" );

    private static final Logger s_logger = Logger.getLogger( ThreadPool.class.getName() );
    private static final Runnable LOCK = new DummyRunnable();
    private static final int FS_PADDING = 16;
    private static final int IDLE_THREADS_MASK = 0x1FFFFFFF;
    private static final int STATE_STOP = 0x40000000;
    private static final int STATE_SPIN = 0x20000000;

    private final String m_name;
    private final int m_contentionFactor;
    private final Thread [] m_thread;
    private final AtomicReferenceArray<Runnable> m_hra;
    private final AtomicReferenceArray<Runnable> m_tra;
    private volatile int m_state;

    public ThreadPool( String name, int threads, int contentionFactor )
    {
        /* Current implementation supports up to 29 worker threads,
         * should be enough for most cases for now.
         * (640 kB ought to be enough for anybody, yes:)
         */
        if (threads > 29)
            threads = 29;

        assert( contentionFactor >= 1 );
        if (contentionFactor < 1)
            contentionFactor = 1;

        m_name = name;
        m_contentionFactor = contentionFactor;

        m_thread = new Thread[threads];
        for (int idx=0; idx<threads; idx++)
            m_thread[idx] = new Worker(idx);

        m_hra = new AtomicReferenceArray<Runnable>( contentionFactor * FS_PADDING );
        m_tra = new AtomicReferenceArray<Runnable>( contentionFactor * FS_PADDING );
        m_state = 0;
    }

    public ThreadPool( String name, int threads )
    {
        this( name, threads, 4 );
    }

    public void start()
    {
        for (Thread thread : m_thread)
            thread.start();
    }

    public void stopAndWait() throws InterruptedException
    {
        assert( m_thread != null );

        for (;;)
        {
            final int state = s_stateUpdater.get( this );
            assert( (state & STATE_STOP) == 0 );
            if (s_stateUpdater.compareAndSet(this, state, state|STATE_STOP))
                break;
        }

        for (int idx=0; idx<m_thread.length; idx++)
        {
            final int workerId = (1 << idx);
            for (;;)
            {
                final int state = s_stateUpdater.get(this);
                if ((state & workerId) == 0)
                    break;
                if (s_stateUpdater.compareAndSet(this, state, state^workerId))
                {
                    LockSupport.unpark( m_thread[idx] );
                    break;
                }
            }
        }

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

        for (;;)
        {
            final int state = s_stateUpdater.get( this );
            if ((state & IDLE_THREADS_MASK) == 0)
            {
                if ((state & STATE_SPIN) != 0)
                    break;
                if (s_stateUpdater.compareAndSet(this, state, state|STATE_SPIN))
                    break;
            }
            else
            {
                int workerIdx;
                if ((state & 1) == 0)
                {
                    workerIdx = 1;
                    int v = state;
                    if ((v & 0xFFFF) == 0) { workerIdx += 16; v >>= 16; }
                    if ((v & 0xFF) == 0) { workerIdx += 8; v >>= 8; }
                    if ((v & 0xF) == 0) { workerIdx += 4; v >>= 4; }
                    if ((v & 0x3) == 0) { workerIdx += 2; v >>= 2; }
                    workerIdx -= (v & 1);
                }
                else
                    workerIdx = 0;

                final int newState = (state ^ (1 << workerIdx));
                if (s_stateUpdater.compareAndSet(this, state, newState))
                {
                    LockSupport.unpark( m_thread[workerIdx] );
                    break;
                }
            }
        }
    }
}
