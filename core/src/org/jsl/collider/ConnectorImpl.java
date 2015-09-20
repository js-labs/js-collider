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

class ConnectorImpl
        extends SessionEmitterImpl
        implements ColliderImpl.ChannelHandler
{
    private static final Logger s_logger = Logger.getLogger( Connector.class.getName() );

    private final Connector m_connector;
    private final Selector m_selector;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;

    private final ReentrantLock m_lock;
    private final Condition m_cond;
    private Thread m_callbackThread;
    private boolean m_stop;
    private int m_state;
    private int m_waiters;

    private final static int STARTING_0 = 0;
    private final static int STARTING_1 = 1;
    private final static int STARTING_2 = 2;
    private final static int CONNECTING = 3;
    private final static int STOPPED    = 4;

    private boolean setState( int newState )
    {
        m_lock.lock();
        try
        {
            if (m_stop)
            {
                m_state = STOPPED;
                if (m_waiters > 0)
                    m_cond.signalAll();
                return false;
            }
            m_state = newState;
            return true;
        }
        finally
        {
            m_lock.unlock();
        }
    }

    private class Starter1 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            if (setState(STARTING_1))
            {
                IOException thrown;
                try
                {
                    m_socketChannel = SocketChannel.open();
                    m_socketChannel.configureBlocking( false );
                    final boolean connected = m_socketChannel.connect( m_connector.getAddr() );
                    m_collider.executeInSelectorThread( new Starter2(connected) );
                    return;
                }
                catch (final IOException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.log(Level.WARNING, m_connector.getAddr() + ": " + ex + ".");
                    thrown = ex;
                }

                final Thread currentThread = Thread.currentThread();
                addThread( currentThread );
                m_connector.onException( thrown );
                removeThreadAndReleaseMonitor( currentThread );

                if (m_socketChannel != null)
                {
                    try { m_socketChannel.close(); }
                    catch (final IOException ex) { logException( ex ); }
                    m_socketChannel = null;
                }
            }
            else
            {
                /* Nothing to cleanup here */
                if (s_logger.isLoggable(Level.FINE))
                    s_logger.log( Level.FINE, m_connector.getAddr() + ": stop STARTING_0->STARTING_1" );
            }
        }
    }

    private class Starter2 extends ColliderImpl.SelectorThreadRunnable
    {
        private final boolean m_connected;

        public Starter2( boolean connected )
        {
            m_connected = connected;
        }

        public int runInSelectorThread()
        {
            if (setState(STARTING_2))
            {
                IOException thrown = null;
                try
                {
                    m_selectionKey = m_socketChannel.register( m_selector, 0, null );
                }
                catch (final IOException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( m_connector.getAddr() + ": " + ex + "." );
                    thrown = ex;
                }

                if (thrown == null)
                {
                    if (m_connected)
                    {
                        m_collider.executeInThreadPool( new Starter4(m_socketChannel, m_selectionKey) );
                        m_socketChannel = null;
                        m_selectionKey = null;
                    }
                    else if (setState(CONNECTING))
                    {
                        m_selectionKey.interestOps( SelectionKey.OP_CONNECT );
                        m_selectionKey.attach( ConnectorImpl.this );
                    }
                    else
                    {
                        /* Connector is being closed. */
                        m_selectionKey.cancel();
                        m_selectionKey = null;

                        try { m_socketChannel.close(); }
                        catch (final IOException ex) { logException( ex ); }
                        m_socketChannel = null;
                    }
                }
                else
                {
                    final Thread currentThread = Thread.currentThread();
                    addThread( currentThread );
                    m_connector.onException( thrown );
                    removeThreadAndReleaseMonitor( currentThread );

                    try { m_socketChannel.close(); }
                    catch (final IOException ex) { logException( ex ); }
                    m_socketChannel = null;
                }
            }
            else
            {
                /* Connector is being closed. */
                try { m_socketChannel.close(); }
                catch (final IOException ex) { logException( ex ); }
                m_socketChannel = null;

                if (s_logger.isLoggable(Level.FINE))
                    s_logger.log( Level.FINE, m_connector.getAddr() + ": stop STARTING_1->STARTING_2" );
            }
            return 0;
        }
    }

    /* We receive socket connection notification in selector thread,
     * but would be better to release selector thread a soon as possible.
     * Starter3 creates and initialize a new session in the ThreadPool.
     */
    private class Starter3 extends ThreadPool.Runnable
    {
        private final SocketChannel m_socketChannel;
        private final SelectionKey m_selectionKey;

        public Starter3( SocketChannel socketChannel, SelectionKey selectionKey )
        {
            m_socketChannel = socketChannel;
            m_selectionKey = selectionKey;
        }

        public void runInThreadPool()
        {
            IOException thrown = null;
            boolean connected = false;
            try
            {
                connected = m_socketChannel.finishConnect();
            }
            catch (final IOException ex)
            {
                thrown = ex;
            }

            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.log( Level.FINE, m_connector.getAddr() + ": " +
                        ConnectorImpl.this.toString() + ": " +
                        " connected=" + connected + " thrown=" + thrown );
            }

            if ((thrown == null) && connected)
                startSession( m_socketChannel, m_selectionKey );
            else
            {
                final Thread currentThread = Thread.currentThread();
                addThread( currentThread );
                m_connector.onException( thrown );
                removeThreadAndReleaseMonitor( currentThread );

                m_selectionKey.cancel();

                try { m_socketChannel.close(); }
                catch (final IOException ex) { logException( ex ); }
            }
        }
    }

    private class Starter4 extends ThreadPool.Runnable
    {
        private final SocketChannel m_socketChannel;
        private final SelectionKey m_selectionKey;

        public Starter4( SocketChannel socketChannel, SelectionKey selectionKey )
        {
            m_socketChannel = socketChannel;
            m_selectionKey = selectionKey;
        }

        public void runInThreadPool()
        {
            startSession( m_socketChannel, m_selectionKey );
        }
    }

    private class Stopper extends ColliderImpl.SelectorThreadRunnable
    {
        public int runInSelectorThread()
        {
            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.log( Level.FINE,
                    m_connector.getAddr() + ": " + ConnectorImpl.this.toString() );
            }

            if (m_selectionKey != null)
            {
                assert( (m_selectionKey.interestOps() & SelectionKey.OP_CONNECT) != 0 );
                m_lock.lock();
                try
                {
                    assert (m_state == CONNECTING);
                    m_state = STOPPED;
                    if (m_waiters > 0)
                        m_cond.signalAll();
                }
                finally
                {
                    m_lock.unlock();
                }

                m_selectionKey.cancel();
                m_selectionKey = null;

                try { m_socketChannel.close(); }
                catch (final IOException ex) { logException(ex); }
                m_socketChannel = null;
            }
            return 0;
        }
    }

    protected void addThread( Thread thread )
    {
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
        m_lock.lock();
        try
        {
            assert( m_callbackThread == thread );

            m_callbackThread = null;
            m_state = STOPPED;

            if (m_waiters > 0)
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
            final StringWriter sw = new StringWriter();
            ex.printStackTrace( new PrintWriter(sw) );
            s_logger.log( Level.WARNING, m_connector.getAddr() + ":\n" + sw.toString() );
        }
    }

    public ConnectorImpl(
            ColliderImpl collider,
            RetainableDataBlockCache inputQueueDataBlockCache,
            Connector connector,
            int joinMessageMaxSize,
            RetainableByteBufferPool joinPool,
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

    public void start()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.log( Level.FINE, m_connector.getAddr().toString() );
        m_collider.executeInThreadPool( new Starter1() );
    }

    public void stopAndWait() throws InterruptedException
    {
        /* Could be much simpler,
         * but would be better to have a possibility to stop the Connector
         * while Collider.run() is not started yet.
         */
        int state;

        m_lock.lock();
        try
        {
            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.log( Level.FINE, m_connector.getAddr() +
                        ": " + this.toString() + ": state=" + m_state + " stop=" + m_stop );
            }

            state = m_state;
            if (m_state == STARTING_0)
            {
                m_stop = true;
                m_state = STOPPED;
            }
            else if ((m_state == STARTING_1) || (m_state == STARTING_2))
            {
                m_stop = true;
                if (m_callbackThread != Thread.currentThread())
                {
                    m_waiters++;
                    while (m_state != STOPPED)
                        m_cond.await();
                    m_waiters--;
                }
            }
            else if (m_state == CONNECTING)
            {
                if (m_callbackThread != Thread.currentThread())
                {
                    if (m_stop)
                    {
                        m_waiters++;
                        while (m_state != STOPPED)
                            m_cond.await();
                        m_waiters--;
                        state = -1;
                    }
                    else
                        m_stop = true;
                }
                else
                {
                    /* Called from the Connector.onException,
                     * do nothing, connector is being stopped anyway.
                     */
                    state = -1;
                }
            }
        }
        finally
        {
            m_lock.unlock();
        }

        if (state == STARTING_0)
        {
            m_collider.removeEmitterNoWait(m_connector);
        }
        else if (state == CONNECTING)
        {
            m_collider.executeInSelectorThread( new Stopper() );
            m_lock.lock();
            try
            {
                m_waiters++;
                while (m_state != STOPPED)
                    m_cond.await();
                m_waiters--;
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
        m_selectionKey.interestOps( 0 );
        m_collider.executeInThreadPool( new Starter3(m_socketChannel, m_selectionKey) );
        m_socketChannel = null;
        m_selectionKey = null;
        return 0;
    }
}
