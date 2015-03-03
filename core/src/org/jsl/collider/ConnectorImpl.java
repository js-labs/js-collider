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
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.nio.channels.Selector;


public class ConnectorImpl extends SessionEmitterImpl
        implements ColliderImpl.ChannelHandler
{
    private static final Logger s_logger = Logger.getLogger( "org.jsl.collider.Connector" );

    private final Connector m_connector;
    private final Selector m_selector;
    private SocketChannel m_socketChannel;
    private boolean m_connected;
    private SelectionKey m_selectionKey;

    private final ReentrantLock m_lock;
    private final Condition m_cond;
    private Thread m_callbackThread;
    private boolean m_stop;
    private int m_state;

    private final static int STARTING_0 = 0;
    private final static int STARTING_1 = 1;
    private final static int RUNNING    = 2;
    private final static int STOPPED    = 3;

    private class Starter1 extends ColliderImpl.SelectorThreadRunnable
    {
        private boolean setState( int newState )
        {
            m_lock.lock();
            try
            {
                if (m_stop)
                    return false;
                m_state = newState;
                return true;
            }
            finally
            {
                m_lock.unlock();
            }
        }

        public int runInSelectorThread()
        {
            if (setState(STARTING_1))
            {
                IOException thrown = null;
                try
                {
                    m_selectionKey = m_socketChannel.register( m_selector, 0, null );
                }
                catch (IOException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( m_connector.getAddr() + ": " + ex + "." );
                    thrown = ex;
                }

                if (thrown == null)
                {
                    if (m_connected)
                    {
                        m_collider.executeInThreadPool( new Starter3() );
                        return 0;
                    }
                    else
                    {
                        if (setState(RUNNING))
                        {
                             m_selectionKey.interestOps( SelectionKey.OP_CONNECT );
                             m_selectionKey.attach( ConnectorImpl.this );
                             return 0;
                        }
                        /* Connector is being stopped, selection key not needed. */
                        m_selectionKey.cancel();
                        m_selectionKey = null;
                    }
                }

                try
                {
                    m_socketChannel.close();
                }
                catch (IOException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( m_connector.getAddr() + ": " + ex + "." );
                }
                m_socketChannel = null;

                if (thrown != null)
                    m_connector.onException( thrown );

                m_lock.lock();
                try
                {
                    assert( m_state != STOPPED );
                    m_state = STOPPED;
                    if (m_stop)
                        m_cond.signalAll();
                }
                finally
                {
                    m_lock.unlock();
                }
            }
            return 0;
        }
    }

    private class Starter2 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            IOException thrown = null;
            boolean connected = false;
            try
            {
                connected = m_socketChannel.finishConnect();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.log( Level.WARNING, m_connector.getAddr() + ": " +
                            ConnectorImpl.this.toString() + ": " + ex.toString() );
                }
                thrown = ex;
            }

            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.log( Level.FINE, m_connector.getAddr() + ": " +
                        ConnectorImpl.this.toString() + ": " +
                        " connected=" + connected + " thrown=" + thrown );
            }

            if ((thrown == null) && connected)
            {
                startSession( m_socketChannel, m_selectionKey );
            }
            else
            {
                m_selectionKey.cancel();
                m_selectionKey = null;

                try
                {
                    m_socketChannel.close();
                }
                catch (IOException ex1)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                    {
                        s_logger.log( Level.WARNING, m_connector.getAddr() + ": " +
                                ConnectorImpl.this.toString() + ": " + ex1.toString() );
                    }
                }
                m_socketChannel = null;

                m_collider.removeEmitterNoWait( m_connector );

                if (thrown != null)
                    m_connector.onException( thrown );

                m_lock.lock();
                try
                {
                    assert( m_state != STOPPED );
                    m_state = STOPPED;
                    if (m_stop)
                        m_cond.signalAll();
                }
                finally
                {
                    m_lock.unlock();
                }
            }
        }
    }

    private class Starter3 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            startSession( m_socketChannel, m_selectionKey );
            m_socketChannel = null;
            m_selectionKey = null;
        }
    }

    private class Stopper extends ColliderImpl.SelectorThreadRunnable
    {
        private int m_waits;

        public int runInSelectorThread()
        {
            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.log( Level.FINE,
                    m_connector.getAddr() + ": " + ConnectorImpl.this.toString() +
                    " waits=" + m_waits );
            }

            final int interestOps = m_selectionKey.interestOps();
            if ((interestOps & SelectionKey.OP_CONNECT) == 0)
            {
                /* Connector is being establishing connection,
                 * or already established.
                 * Will change state to the STOPPED soon.
                 */
                return 0;
            }

            /* Unlike the Acceptor we can signal waiter before socket close. */
            m_lock.lock();
            try
            {
                assert( m_stop );
                m_state = STOPPED;
                m_cond.signalAll();
            }
            finally
            {
                m_lock.unlock();
            }

            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.log( Level.FINE,
                        m_connector.getAddr() + ": " + ConnectorImpl.this.toString() +
                        ": waits=" + m_waits + "." );
            }

            m_selectionKey.cancel();
            m_selectionKey = null;

            try
            {
                m_socketChannel.close();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.log( Level.WARNING, m_connector.getAddr() + ": " +
                            ConnectorImpl.this.toString() + ": " + ex.toString() + "." );
                }
            }

            m_socketChannel = null;
            return 0;
        }
    }

    protected void addThread( Thread thread )
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.log( Level.FINE, m_connector.getAddr() + ": " + this.toString() );

        m_lock.lock();
        try
        {
            assert( m_callbackThread == null );
            m_callbackThread = thread;
        }
        finally
        {
            m_lock.unlock();
        }
    }

    protected void removeThreadAndReleaseMonitor( Thread thread )
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.log( Level.FINE, m_connector.getAddr() + ": " + this.toString() );

        m_lock.lock();
        try
        {
            assert( m_callbackThread == thread );
            assert( (m_state == STARTING_1) || (m_state == RUNNING) );
            m_callbackThread = null;
            m_state = STOPPED;
            m_socketChannel = null;
            m_selectionKey = null;

            if (m_stop)
                m_cond.signalAll();
        }
        finally
        {
            m_lock.unlock();
        }

        m_collider.removeEmitterNoWait( m_connector );
    }

    protected void logException( Exception ex )
    {
        if (s_logger.isLoggable(Level.WARNING))
        {
            StringWriter sw = new StringWriter();
            ex.printStackTrace( new PrintWriter(sw) );
            s_logger.log( Level.WARNING, m_connector.getAddr() + ":\n" + sw.toString() );
        }
    }

    public ConnectorImpl(
            ColliderImpl collider,
            DataBlockCache inputQueueDataBlockCache,
            Connector connector,
            int joinMessageMaxSize,
            ByteBufferPool joinPool,
            Selector selector )
    {
        super( collider, inputQueueDataBlockCache, connector, joinMessageMaxSize, joinPool );
        m_connector = connector;
        m_selector = selector;

        m_lock = new ReentrantLock();
        m_cond = m_lock.newCondition();
        m_stop = false;
        m_state = STARTING_0;
    }

    public final void start( SocketChannel socketChannel, boolean connected )
    {
        /* Can be called after stopAndWait() */
        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.log( Level.FINE,
                    m_connector.getAddr() + ": (" + (connected ? "connected" : "pending") +
                    ") " + this.toString() + "." );
        }

        m_socketChannel = socketChannel;
        m_connected = connected;

        /* Even if socket channel is connected,
         * we still need to create SelectionKey.
         */
        m_collider.executeInSelectorThread( new Starter1() );
    }

    public void stopAndWait() throws InterruptedException
    {
        /* Could be much simpler,
         * but we need a possibility to stop the Connector
         * while collider is not started yet.
         */
        final Thread currentThread = Thread.currentThread();
        int state;

        m_lock.lock();
        try
        {
            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.log( Level.FINE, m_connector.getAddr() +
                        ": " + this.toString() + ": state=" + m_state + " stop=" + m_stop );
            }

            if (m_state == STARTING_0)
            {
                if (m_stop)
                    return;
                m_stop = true;
                state = 0;
            }
            else if (m_state == STOPPED)
            {
                return;
            }
            else
            {
                if (m_callbackThread == currentThread)
                {
                    /* Called from the createSessionListener() callback. */
                    m_stop = true;
                    return;
                }

                if (m_stop)
                {
                    while (m_state != STOPPED)
                        m_cond.await();
                    return;
                }
                m_stop = true;
                state = 1;
            }
        }
        finally
        {
            m_lock.unlock();
        }

        if (state == 0)
        {
            if (s_logger.isLoggable(Level.FINE))
                s_logger.log( Level.FINE, m_connector.getAddr() + ": " + this.toString() );

            /* Socket channel is not registered in the selector yet,
             * and will not be registered, m_selectionKey should be null.
             */
            assert( m_selectionKey == null );

            try
            {
                m_socketChannel.close();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_connector.getAddr() + ": " + ex + "." );
            }
            m_socketChannel = null;

            m_collider.removeEmitterNoWait( m_connector );
        }
        else
        {
            m_collider.executeInSelectorThread( new Stopper() );
            m_lock.lock();
            try
            {
                while (m_state != STOPPED)
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
        assert( m_selectionKey.readyOps() == SelectionKey.OP_CONNECT );
        m_collider.executeInThreadPool( new Starter2() );
        m_selectionKey.interestOps(0);
        return 0;
    }
}
