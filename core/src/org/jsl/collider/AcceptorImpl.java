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
import java.net.SocketAddress;
import java.nio.channels.Selector;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;


public class AcceptorImpl extends SessionEmitterImpl
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
                    socketChannel = m_channel.accept();
                    if (socketChannel == null)
                        break;
                }
                catch (IOException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( ex.toString() );
                    break;
                }

                try
                {
                    socketChannel.configureBlocking( false );
                }
                catch (IOException ex)
                {
                    /* Having unblocking mode is critical, can't work with this socket. */
                    try
                    {
                        socketChannel.close();
                    }
                    catch (IOException ex1)
                    {
                        if (s_logger.isLoggable(Level.WARNING))
                        {
                            s_logger.warning( m_localAddr.toString() +
                                    ": SocketChanel.close() failed: " + ex1.toString() );
                        }
                    }

                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( m_localAddr.toString() + ": " + ex.toString() + "." );

                    continue;
                }

                startSession( m_selector, socketChannel );

                if ((m_state.get() & FL_STOP) != 0)
                    break;
            }

            m_collider.executeInSelectorThread( m_starter3 );
        }
    }

    private class Starter1 extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            int state = m_state.get();
            int newState;

            for (;;)
            {
                if ((state & FL_STARTING) == 0)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                    {
                        s_logger.warning( m_localAddr.toString() +
                                ": internal error: state=" + state + "." );
                    }
                }

                if ((state & FL_STOP) == 0)
                {
                    newState = 0;
                    if (m_state.compareAndSet(state, newState))
                    {
                        try
                        {
                            m_selectionKey = m_channel.register( m_selector, 0, AcceptorImpl.this );
                            m_collider.executeInThreadPool( new Starter2() );
                        }
                        catch (IOException ex)
                        {
                            m_collider.executeInThreadPool( new ExceptionNotifier(ex, m_channel) );
                            m_channel = null;
                        }
                        break;
                    }
                }
                else
                {
                    /* Acceptor is stopped and nobody waits it. */
                    try
                    {
                        m_channel.close();
                    }
                    catch (IOException ex)
                    {
                        if (s_logger.isLoggable(Level.WARNING))
                            s_logger.warning( m_localAddr.toString() + ": " + ex.toString() + "." );
                    }
                    m_channel = null;
                    newState = state;
                    break;
                }

                state = m_state.get();
            }

            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.fine( m_localAddr.toString() +
                        ": state=(" + state + " -> " + newState + ")." );
            }
        }
    }

    private class Starter2 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            Thread currentThread = Thread.currentThread();
            addThread( currentThread );
            try
            {
                m_acceptor.onAcceptorStarted( m_collider, m_channel.socket().getLocalPort() );
                m_collider.executeInSelectorThread( m_starter3 );
            }
            finally
            {
                removeThread( currentThread );
            }
        }
    }

    private class Starter3 extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (m_selectionKey != null)
            {
                assert( m_selectionKey.interestOps() == 0 );
                m_selectionKey.interestOps( SelectionKey.OP_ACCEPT );
            }
            else
            {
                releaseMonitor();

                try
                {
                    m_channel.close();
                }
                catch (IOException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( m_localAddr.toString() + ": " + ex.toString() + "." );
                }
                m_channel = null;
            }
        }
    }

    private class Stopper extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            int interestOps = m_selectionKey.interestOps();
            m_selectionKey.cancel();
            m_selectionKey = null;

            if (s_logger.isLoggable(Level.FINE))
                s_logger.fine( m_localAddr.toString() + ": interestOps=" + interestOps + "." );

            if ((interestOps & SelectionKey.OP_ACCEPT) == 0)
            {
                /* Starter3 will be executed soon. */
            }
            else
            {
                releaseMonitor();

                try
                {
                    m_channel.close();
                }
                catch (IOException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( m_localAddr.toString() + ": " + ex.toString() + "." );
                }
                m_channel = null;
            }
        }
    }

    private static final Logger s_logger = Logger.getLogger( AcceptorImpl.class.getName() );
    private static final int FL_STARTING = 0x0001;
    private static final int FL_STOP     = 0x0002;

    private final Acceptor m_acceptor;
    private final Selector m_selector;
    private final SocketAddress m_localAddr;
    private ServerSocketChannel m_channel;
    private SelectionKey m_selectionKey;
    private final AtomicInteger m_state;
    private final ChannelAcceptor m_channelAcceptor;
    private final Starter3 m_starter3;

    public AcceptorImpl(
            ColliderImpl collider,
            DataBlockCache inputQueueDataBlockCache,
            Acceptor acceptor,
            Selector selector,
            ServerSocketChannel channel )
    {
        super( collider, inputQueueDataBlockCache, acceptor );

        m_acceptor = acceptor;
        m_selector = selector;
        m_localAddr = channel.socket().getLocalSocketAddress();

        m_channel = channel;
        m_state = new AtomicInteger( FL_STARTING );
        m_channelAcceptor = new ChannelAcceptor();
        m_starter3 = new Starter3();
    }

    public final void start()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( m_localAddr.toString() );

        m_collider.executeInSelectorThread( new Starter1() );
    }

    public void stopAndWait() throws InterruptedException
    {
        /* Could be simpler,
         * but we need a possibility to stop the acceptor
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
                        m_collider.executeInSelectorThread( new Stopper() );
                        break;
                    }
                    else
                    {
                        /* Acceptor is not started yet, no need to wait. */
                        if (s_logger.isLoggable(Level.FINE))
                            s_logger.fine( m_localAddr + ": state=" + state + "." );
                        return;
                    }
                }
            }
            else
            {
                if ((state & FL_STARTING) == 0)
                {
                    newState = state;
                    break;
                }
                else
                {
                    /* Acceptor is not even started yet, no need to wait. */
                    if (s_logger.isLoggable(Level.FINE))
                        s_logger.fine( m_localAddr.toString() + ": state=" + state + "." );
                    return;
                }
            }
            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( m_localAddr.toString() + ": state=(" + state + " -> " + newState + ")." );

        waitMonitor();
    }

    public void handleReadyOps( ThreadPool threadPool )
    {
        m_selectionKey.interestOps( 0 );
        threadPool.execute( m_channelAcceptor );
    }
}
