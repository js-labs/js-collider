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

    private static final int FL_START = 0x0001;
    private static final int FL_STOP  = 0x0002;

    private final Connector m_connector;
    private Selector m_selector;
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
                assert( (state & FL_START) != 0 );

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
            m_collider.executeInThreadPool( new Starter2() );
        }
    }

    private class Starter2 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            try
            {
                m_socketChannel.finishConnect();

                SessionImpl sessionImpl = new SessionImpl(
                        m_collider,
                        m_sessionEmitter,
                        m_socketChannel,
                        m_selectionKey,
                        m_outputQueueDataBlockCache );

                m_socketChannel = null;
                m_selectionKey = null;

                Session.Listener sessionListener = m_sessionEmitter.createSessionListener( sessionImpl );
                sessionImpl.initialize( m_inputQueueDataBlockCache, sessionListener );
            }
            catch (IOException ex)
            {
                m_connector.onException( ex );
                m_collider.executeInSelectorThread( new Stopper2() );
                /* monitor will be released in the 'finally' block. */

                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( m_connector.getAddr().toString() + ": " + ex.toString() );
            }
            finally
            {
                int pendingOps = releaseMonitor();
                if (pendingOps != 1)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( "internal error: pendingOps=" + pendingOps + "." );
                }
            }
        }
    }

    private class Stopper1 extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (m_selectionKey != null)
            {
                int interestOps = m_selectionKey.interestOps();
                if ((interestOps & SelectionKey.OP_CONNECT) != 0)
                {
                    /* Socket is not connected yet. */
                    int pendingOps = releaseMonitor();
                    if (pendingOps != 1)
                    {
                        if (s_logger.isLoggable(Level.WARNING))
                        {
                            s_logger.warning(
                                    m_connector.getAddr().toString() +
                                    ": internal error: pendingOps=" + pendingOps + "." );
                        }
                    }
                    closeSelectionKeyAndChannel();
                }
            }
        }
    }

    private class Stopper2 extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            closeSelectionKeyAndChannel();
        }
    }

    private void closeSelectionKeyAndChannel()
    {
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

    public ConnectorImpl(
            ColliderImpl collider,
            Connector connector,
            Selector selector,
            InputQueue.DataBlockCache inputQueueDataBlockCache,
            OutputQueue.DataBlockCache outputQueueDataBlockCache )
    {
        super( collider, connector, inputQueueDataBlockCache, outputQueueDataBlockCache );
        m_connector = connector;
        m_selector = selector;
        m_state = new AtomicInteger( FL_START );
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
        int state = m_state.get();
        int newState;
        for (;;)
        {
            if ((state & FL_START) == 0)
            {
                if ((state & FL_STOP) != 0)
                {
                    newState = state;
                    break;
                }

                newState = (state | FL_STOP);
                if (m_state.compareAndSet(state, newState))
                {
                    m_collider.executeInSelectorThread( new Stopper1() );
                    break;
                }
            }
            else
            {
                if ((state & FL_STOP) != 0)
                {
                    if (s_logger.isLoggable(Level.FINE))
                        s_logger.fine( m_connector.getAddr().toString() + ": state=" + state );
                    return;
                }

                newState = (state | FL_STOP);
                if (m_state.compareAndSet(state, newState))
                {
                    if (s_logger.isLoggable(Level.FINE))
                    {
                        s_logger.fine(
                                m_connector.getAddr().toString() +
                                ": state=(" + state + " -> " + newState + ")." );
                    }
                    m_collider.removeEmitterNoWait( m_connector );
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
