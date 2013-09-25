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

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.nio.channels.Selector;


public class ConnectorImpl extends SessionEmitterImpl
{
    private static final Logger s_logger = Logger.getLogger( AcceptorImpl.class.getName() );

    private static final int FL_STARTING = 0x0001;
    private static final int FL_STOP     = 0x0002;

    private final Connector m_connector;
    private final Selector m_selector;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private final AtomicInteger m_state;

    private class Starter1 extends ColliderImpl.SelectorThreadRunnable
             implements ColliderImpl.ChannelHandler
    {
        public void runInSelectorThread()
        {
            for (;;)
            {
                int state = m_state.get();

                if ((state & FL_STARTING) == 0)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                    {
                        s_logger.warning(
                                m_connector.getAddr().toString() +
                                ": internal error: state=" + state + "." );
                    }
                }

                if ((state & FL_STOP) == 0)
                {
                    if (m_state.compareAndSet(state, 0))
                    {
                        try
                        {
                            m_selectionKey = m_socketChannel.register(
                                    m_selector, SelectionKey.OP_CONNECT, this );
                        }
                        catch (IOException ex)
                        {
                            m_collider.executeInThreadPool( new ExceptionNotifier(ex, m_socketChannel) );
                            m_socketChannel = null;
                        }
                        break;
                    }
                }
                else
                {
                    try
                    {
                        m_socketChannel.close();
                    }
                    catch (IOException ex)
                    {
                        if (s_logger.isLoggable(Level.WARNING))
                            s_logger.warning( m_connector.getAddr().toString() + ": " + ex.toString() );
                    }
                    m_socketChannel = null;
                    break;
                }
            }
        }

        public void handleReadyOps( ThreadPool threadPool )
        {
            m_selectionKey.interestOps(0);

            int readyOps = m_selectionKey.readyOps();
            if (readyOps != SelectionKey.OP_CONNECT)
            {
                if (s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.warning(
                            m_connector.getAddr().toString() +
                            ": internal error: readyOps=" + readyOps + "." );
                }
            }

            Starter2 starter2 = new Starter2( m_socketChannel, m_selectionKey );
            m_selectionKey = null;
            m_socketChannel = null;

            m_collider.executeInThreadPool( starter2 );
        }
    }

    private class Starter2 extends ThreadPool.Runnable
    {
        private final SocketChannel m_socketChannel;
        private final SelectionKey m_selectionKey;

        public Starter2( SocketChannel socketChannel, SelectionKey selectionKey  )
        {
            m_socketChannel = socketChannel;
            m_selectionKey = selectionKey;
        }

        public void runInThreadPool()
        {
            try
            {
                m_socketChannel.finishConnect();
            }
            catch (IOException ex)
            {
                int pendingOps = releaseMonitor();
                if ((pendingOps != 1) && s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( "internal error: pendingOps=" + pendingOps + "." );

                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_connector.getAddr().toString() + ": " + ex.toString() );

                m_connector.onException( ex );

                Stopper2 stopper2 = new Stopper2( m_socketChannel, m_selectionKey );
                m_collider.executeInSelectorThread( stopper2 );
                return;
            }

            SessionImpl sessionImpl = new SessionImpl(
                        m_collider,
                        m_connector,
                        m_socketChannel,
                        m_selectionKey,
                        m_outputQueueDataBlockCache );

            Thread currentThread = Thread.currentThread();
            addThread( currentThread );
            try
            {
                Session.Listener sessionListener = m_connector.createSessionListener( sessionImpl );
                sessionImpl.initialize( m_inputQueueDataBlockCache, sessionListener );
            }
            finally
            {
                removeThreadAndReleaseMonitor( currentThread );
            }
        }
    }

    private class Stopper1 extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if ((m_selectionKey != null) &&
                (m_selectionKey.interestOps() == SelectionKey.OP_CONNECT))
            {
                /* Socket is not connected yet. */
                int pendingOps = releaseMonitor();
                if ((pendingOps != 1) && s_logger.isLoggable(Level.WARNING))
                {
                    s_logger.warning(
                            m_connector.getAddr().toString() +
                            ": internal error: pendingOps=" + pendingOps + "." );
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
                        s_logger.warning( m_connector.getAddr().toString() + ": " + ex.toString() );
                }
                m_socketChannel = null;
            }
        }
    }

    private class Stopper2 extends ColliderImpl.SelectorThreadRunnable
    {
        private final SocketChannel m_socketChannel;
        private final SelectionKey m_selectionKey;

        public Stopper2( SocketChannel socketChannel, SelectionKey selectionKey )
        {
            m_socketChannel = socketChannel;
            m_selectionKey = selectionKey;
        }

        public void runInSelectorThread()
        {
            m_selectionKey.cancel();
            try
            {
                m_socketChannel.close();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_connector.getAddr().toString() + ": " + ex.toString() );
            }
        }
    }

    public ConnectorImpl(
            ColliderImpl collider,
            InputQueue.DataBlockCache inputQueueDataBlockCache,
            OutputQueue.DataBlockCache outputQueueDataBlockCache,
            Connector connector,
            Selector selector )
    {
        super( collider, inputQueueDataBlockCache, outputQueueDataBlockCache, connector );
        m_connector = connector;
        m_selector = selector;
        m_state = new AtomicInteger( FL_STARTING );
    }

    public final void start( SocketChannel socketChannel, boolean connected )
    {
        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine(
                    m_connector.getAddr().toString() +
                    ": (" + (connected ? "connected" : "pending") + ")." );
        }

        if (connected)
        {
            startSession( m_selector, socketChannel );
            releaseMonitor();
        }
        else
        {
            m_socketChannel = socketChannel;
            m_collider.executeInSelectorThread( new Starter1() );
        }
    }

    public void stopAndWait() throws InterruptedException
    {
        /* Could be simpler,
         * but we need a possibility to stop the connector
         * while collider is not started yet.
         */
        int state = m_state.get();
        int newState;
        for (;;)
        {
            if ((state & FL_STOP) == 0)
            {
                newState = (state | FL_STOP);
                if (m_state.compareAndSet(state, newState))
                {
                    if ((newState & FL_STARTING) == 0)
                    {
                        m_collider.executeInSelectorThread( new Stopper1() );
                        break;
                    }
                    else
                    {
                        if (s_logger.isLoggable(Level.FINE))
                        {
                            s_logger.fine(
                                    m_connector.getAddr().toString() +
                                    ": state=(" + state + " -> " + newState + ")." );
                        }

                        int pendingOps = releaseMonitor();
                        if ((pendingOps != 1) && s_logger.isLoggable(Level.WARNING))
                        {
                            s_logger.warning(
                                    m_connector.getAddr().toString() +
                                    ": internal error: pendingOps=" + pendingOps + "." );
                        }
                        return;
                    }
                }
            }
            else
            {
                if ((state & FL_STARTING) == 0)
                {
                    /* Connector is being stop, let's just wait. */
                    newState = state;
                    break;
                }
                else
                {
                    if (s_logger.isLoggable(Level.FINE))
                        s_logger.fine( m_connector.getAddr().toString() + ": state=" + state );
                    return;
                }
            }

            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine(
                    m_connector.getAddr().toString() +
                    ": state=(" + state + " -> " + newState + ")." );
        }

        waitMonitor();
    }
}
