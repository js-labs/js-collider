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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.HashSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


class AcceptorImpl extends SessionEmitterImpl
        implements ColliderImpl.ChannelHandler
{
    private class ChannelAcceptor extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            for (;;)
            {
                SocketChannel socketChannel;

                try
                {
                    socketChannel = m_serverChannel.accept();
                    if (socketChannel == null)
                        break;
                }
                catch (final IOException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( m_localAddr + ": " + ex.toString() );
                    break;
                }

                try
                {
                    socketChannel.configureBlocking( false );
                }
                catch (final IOException ex)
                {
                    /* Having unblocking mode is critical,
                     * can't work with this socket.
                     */
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( m_localAddr + ": " + ex.toString() );

                    try
                    {
                        socketChannel.close();
                    }
                    catch (final IOException ex1)
                    {
                        if (s_logger.isLoggable(Level.WARNING))
                            s_logger.warning( m_localAddr + ": " + ex1.toString() );
                    }
                    continue;
                }

                boolean stop;
                m_lock.lock();
                try
                {
                    stop = m_stop;
                    if (stop)
                        m_stopped = true;
                    else
                        m_pendingOps++;
                }
                finally
                {
                    m_lock.unlock();
                }

                if (stop)
                {
                    /* Acceptor is being closed, socket channel not needed. */
                    try
                    {
                        socketChannel.close();
                    }
                    catch (final IOException ex)
                    {
                        if (s_logger.isLoggable(Level.FINE))
                            s_logger.fine( m_localAddr + ": " + ex.toString() + "." );
                    }
                    return;
                }

                m_collider.executeInSelectorThread( new SessionStarter1(socketChannel) );
            }

            m_collider.executeInSelectorThread( m_starter3 );
        }
    }

    private class SessionStarter1 extends ColliderImpl.SelectorThreadRunnable
    {
        private final SocketChannel m_socketChannel;

        public SessionStarter1( SocketChannel socketChannel )
        {
            m_socketChannel = socketChannel;
        }

        public int runInSelectorThread()
        {
            try
            {
                final SelectionKey selectionKey = m_socketChannel.register( m_selector, 0, null );
                m_collider.executeInThreadPool( new SessionStarter2(m_socketChannel, selectionKey) );
            }
            catch (final IOException ex)
            {
                /* Not necessary a framework problem,
                 * can happen in a case if peer closed connection.
                 */
                if (s_logger.isLoggable(Level.FINE))
                    s_logger.fine( m_localAddr + ": " + ex );
                releaseMonitor();
            }
            return 0;
        }
    }

    private class SessionStarter2 extends ThreadPool.Runnable
    {
        private final SocketChannel m_socketChannel;
        private final SelectionKey m_selectionKey;

        public SessionStarter2( SocketChannel socketChannel, SelectionKey selectionKey )
        {
            m_socketChannel = socketChannel;
            m_selectionKey = selectionKey;
        }

        public void runInThreadPool()
        {
            startSession( m_socketChannel, m_selectionKey );
        }
    }

    private class Starter1 extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            m_lock.lock();
            try
            {
                assert( m_state == STARTING_0 );
                if (m_stop)
                    return 0;
                m_state = STARTING_1;
                m_pendingOps = 1;
            }
            finally
            {
                m_lock.unlock();
            }

            try
            {
                m_selectionKey = m_serverChannel.register( m_selector, 0, AcceptorImpl.this );
                m_collider.executeInThreadPool( new Starter2() );
                return 0;
            }
            catch (final IOException ex)
            {
                /* Any exception here means a bug in Collider framework. */
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_localAddr + ": " + ex + "." );
            }

            try
            {
                m_serverChannel.close();
            }
            catch (final IOException ex1)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_localAddr + ": " + ex1 + "." );
            }
            m_serverChannel = null;

            m_lock.lock();
            try
            {
                m_pendingOps = 0;
                m_cond.signalAll();
            }
            finally
            {
                m_lock.unlock();
            }

            return 0;
        }
    }

    private class Starter2 extends ThreadPool.Runnable
    {
        private void closeAndSignal()
        {
            /* Some threads waits in stopAndWait(),
             * but we need to close selection key
             * and server socket channel first.
             */
            m_selectionKey.cancel();
            m_selectionKey = null;

            try { m_serverChannel.close(); }
            catch (final IOException ex) { logException(ex); }
            m_serverChannel = null;

            m_lock.lock();
            try
            {
                assert( m_pendingOps == 1 );
                m_pendingOps = 0;
                m_cond.signalAll();
            }
            finally
            {
                m_lock.unlock();
            }
        }

        private boolean setStarting2( Thread thread )
        {
            m_lock.lock();
            try
            {
                assert( m_state == STARTING_1 );
                assert( m_pendingOps == 1 );
                if (!m_stop)
                {
                    m_callbackThreads.add( thread );
                    m_state = STARTING_2;
                    return true;
                }
            }
            finally
            {
                m_lock.unlock();
            }
            closeAndSignal();
            return false;
        }

        private boolean setRunning( Thread thread )
        {
            m_lock.lock();
            try
            {
                assert( m_state == STARTING_2 );
                if (!m_callbackThreads.remove(thread))
                {
                    assert( m_stop );
                    return false;
                }
                else if (!m_stop)
                {
                    m_state = RUNNING;
                    return true;
                }
            }
            finally
            {
                m_lock.unlock();
            }
            closeAndSignal();
            return false;
        }

        public void runInThreadPool()
        {
            final Thread currentThread = Thread.currentThread();
            if (setStarting2(currentThread))
            {
                m_acceptor.onAcceptorStarted( m_collider, m_serverChannel.socket().getLocalPort() );
                if (setRunning(currentThread))
                    m_collider.executeInSelectorThread( m_starter3 );
            }
        }
    }

    private class Starter3 extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            assert( m_selectionKey.interestOps() == 0 );
            m_selectionKey.interestOps( SelectionKey.OP_ACCEPT );
            return 0;
        }
    }

    private class Stopper extends ColliderImpl.SelectorThreadRunnable
    {
        private int m_waits;

        public int runInSelectorThread()
        {
            final int interestOps = m_selectionKey.interestOps();

            if ((interestOps & SelectionKey.OP_ACCEPT) == 0)
            {
                boolean stopped;
                m_lock.lock();
                try
                {
                    stopped = m_stopped;
                }
                finally
                {
                    m_lock.unlock();
                }

                if (!stopped)
                {
                    m_waits++;
                    m_collider.executeInSelectorThreadLater( this );
                    return 0;
                }
            }

            if (s_logger.isLoggable(Level.FINE))
                s_logger.fine( m_localAddr + ": waits=" + m_waits + "." );

            m_selectionKey.cancel();
            m_selectionKey = null;

            try
            {
                m_serverChannel.close();
            }
            catch (final IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_localAddr + ": " + ex.toString() + "." );
            }
            m_serverChannel = null;

            /* It is better to release monitor after ServerSocketChannel close
             * to avoid possible race if caller of stopAndWait() will try
             * to accept the same address again.
             */
            releaseMonitor();
            return 0;
        }
    }

    private void releaseMonitor()
    {
        m_lock.lock();
        try
        {
            assert( m_pendingOps > 0 );
            final int pendingOps = --m_pendingOps;
            if (pendingOps > 0)
                return;
            m_cond.signalAll();
        }
        finally
        {
            m_lock.unlock();
        }
        m_collider.removeEmitterNoWait( m_acceptor );
    }

    protected void addThread( Thread thread )
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.log( Level.FINE, m_localAddr.toString() );

        m_lock.lock();
        try
        {
            assert( !m_callbackThreads.contains(thread) );
            m_callbackThreads.add( thread );
        }
        finally
        {
            m_lock.unlock();
        }
    }

    protected void removeThreadAndReleaseMonitor( Thread thread )
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.log( Level.FINE, m_localAddr.toString() );

        m_lock.lock();
        try
        {
            if (!m_callbackThreads.remove(thread))
                return;

            assert( m_pendingOps > 0 );
            final int pendingOps = --m_pendingOps;
            if (pendingOps > 0)
                return;

            m_cond.signalAll();
        }
        finally
        {
            m_lock.unlock();
        }
        m_collider.removeEmitterNoWait( m_acceptor );
    }

    protected void logException( Exception ex )
    {
        if (s_logger.isLoggable(Level.WARNING))
        {
            final StringWriter sw = new StringWriter();
            ex.printStackTrace( new PrintWriter(sw) );
            s_logger.log( Level.WARNING, m_localAddr + ":\n" + sw.toString() );
        }
    }

    private static final Logger s_logger = Logger.getLogger( "org.jsl.collider.Acceptor" );

    private final Acceptor m_acceptor;
    private final Selector m_selector;
    private final SocketAddress m_localAddr;
    private ServerSocketChannel m_serverChannel;
    private SelectionKey m_selectionKey;
    private final ChannelAcceptor m_channelAcceptor;
    private final Starter3 m_starter3;

    private final ReentrantLock m_lock;
    private final Condition m_cond;
    private final HashSet<Thread> m_callbackThreads;
    private int m_pendingOps;
    private boolean m_stop;
    private boolean m_stopped;
    private int m_state;

    private static final int STARTING_0 = 0;
    private static final int STARTING_1 = 1;
    private static final int STARTING_2 = 2;
    private static final int RUNNING    = 3;

    public AcceptorImpl(
            ColliderImpl collider,
            RetainableDataBlockCache inputQueueDataBlockCache,
            Acceptor acceptor,
            int joinMessageMaxSize,
            RetainableByteBufferPool joinPool,
            Selector selector,
            ServerSocketChannel serverChannel )
    {
        super( collider, inputQueueDataBlockCache, acceptor, joinMessageMaxSize, joinPool );

        m_acceptor = acceptor;
        m_selector = selector;
        m_localAddr = serverChannel.socket().getLocalSocketAddress();

        m_serverChannel = serverChannel;
        m_channelAcceptor = new ChannelAcceptor();
        m_starter3 = new Starter3();

        m_lock = new ReentrantLock();
        m_cond = m_lock.newCondition();
        m_callbackThreads = new HashSet<Thread>();
        m_pendingOps = 0;
        m_stop = false;
        m_stopped = false;
        m_state = STARTING_0;
    }

    public final void start()
    {
        /* Can be called after stopAndWait() */
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( m_localAddr.toString() );
        m_collider.executeInSelectorThread( new Starter1() );
    }

    public void stopAndWait() throws InterruptedException
    {
        /* Could be much simpler,
         * but we need a possibility to stop the Acceptor while collider is not started yet,
         * from the onAcceptorStarted() and from the createSessionListener() callbacks as well.
         */
        final Thread currentThread = Thread.currentThread();
        int state;

        m_lock.lock();
        try
        {
            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.fine( m_localAddr +
                        ": state=" + m_state + " stop=" + m_stop + " pendingOps=" + m_pendingOps );
            }

            if (m_state == STARTING_0)
            {
                if (m_stop)
                {
                    while (m_pendingOps > 0)
                        m_cond.await();
                    return;
                }
                m_pendingOps = 1;
                m_stop = true;
                state = 0;
            }
            else if (m_state == STARTING_1)
            {
                /* Selection key is being created,
                 * will not take much time, let's wait.
                 */
                assert( !m_callbackThreads.contains(currentThread) );
                m_stop = true; /* Does not matter was it already set or not. */
                while (m_pendingOps > 0)
                    m_cond.await();
                return;
            }
            else if (m_state == STARTING_2)
            {
                /* onAcceptorStarted() is being called. */
                if (m_stop)
                {
                    while (m_pendingOps > 0)
                        m_cond.await();
                }
                else if (!m_callbackThreads.remove(currentThread))
                {
                    m_stop = true;
                    while (m_pendingOps > 0)
                        m_cond.await();
                    return;
                }
                m_stop = true;
                state = 1;
            }
            else if (m_stop)
            {
                /* Acceptor already being stopped. */
                if (m_callbackThreads.remove(currentThread))
                {
                    assert( m_pendingOps > 0 );
                    m_pendingOps--;
                }
                while (m_pendingOps > 0)
                    m_cond.await();
                return;
            }
            else
            {
                assert( !m_stopped );
                m_stop = true;
                state = 2;
            }
        }
        finally
        {
            m_lock.unlock();
        }

        if (s_logger.isLoggable(Level.FINE))
            s_logger.log( Level.FINE, m_localAddr + ": state=" + state );

        if (state == 0)
        {
            /* stopAndWait() called while the Acceptor did not started yet,
             * server socket channel is not registered in the selector yet,
             * and will not be registered, m_selectionKey should be null.
             */
            assert( m_selectionKey == null );

            try
            {
                m_serverChannel.close();
            }
            catch (final IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_localAddr + ": " + ex + "." );
            }
            m_serverChannel = null;

            m_collider.removeEmitterNoWait( m_acceptor );

            m_lock.lock();
            try
            {
                m_pendingOps = 0;
                m_cond.signalAll();
            }
            finally
            {
                m_lock.unlock();
            }
        }
        else if (state == 1)
        {
            /* stopAndWait() called while the Acceptor called onAcceptorStarted() */

            m_selectionKey.cancel();
            m_selectionKey = null;

            try
            {
                m_serverChannel.close();
            }
            catch (final IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_localAddr + ": " + ex + "." );
            }
            m_serverChannel = null;

            m_collider.removeEmitterNoWait( m_acceptor );

            m_lock.lock();
            try
            {
                assert( m_pendingOps == 1 );
                m_pendingOps = 0;
                m_cond.signalAll();
            }
            finally
            {
                m_lock.unlock();
            }
        }
        else /* (state == 2) */
        {
            m_collider.executeInSelectorThread( new Stopper() );

            m_lock.lock();
            try
            {
                if (m_callbackThreads.remove(currentThread))
                {
                    assert( m_pendingOps > 0 );
                    m_pendingOps--;
                }
                while (m_pendingOps > 0)
                    m_cond.await();
            }
            finally
            {
                m_lock.unlock();
            }
        }
    }

    public int handleReadyOps( ThreadPool threadPool )
    {
        assert( m_selectionKey.readyOps() == SelectionKey.OP_ACCEPT );
        threadPool.execute( m_channelAcceptor );
        m_selectionKey.interestOps(0);
        return 0;
    }
}
