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
import java.nio.channels.*;
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
                            s_logger.warning( ex1.toString() );
                    }

                    if (s_logger.isLoggable(Level.WARNING))
                        s_logger.warning( ex.toString() );

                    continue;
                }

                startSession( m_selector, socketChannel );

                if (m_stop.get() > 0)
                    break;
            }

            m_collider.executeInSelectorThread( m_starter3 );
        }
    }

    private class Starter1 extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (m_selector != null)
            {
                try
                {
                    m_selectionKey = m_channel.register( m_selector, 0, AcceptorImpl.this );
                    m_collider.executeInThreadPool( new Starter2() );
                }
                catch (IOException ex)
                {
                    m_collider.executeInThreadPool( new ExceptionNotifier(ex, m_channel) );
                }
            }
            /* else { SLT: --- Stopper -- Starter1 --- } */
        }
    }

    private class Starter2 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            Thread currentThread = Thread.currentThread();
            acquireMonitor( currentThread );
            try
            {
                m_acceptor.onAcceptorStarted( m_channel.socket().getLocalPort() );
            }
            finally
            {
                int pendingOps = releaseMonitor( currentThread );
                assert( pendingOps == 1 );
            }
            m_collider.executeInSelectorThread( m_starter3 );
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
                m_collider.executeInThreadPool( new Stopper2() );
            }
        }
    }

    private class Stopper1 extends ColliderImpl.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (m_selectionKey == null)
            {
                /* SLT: --- Stopper -- Starter1 --- */
                m_selector = null;
                int pendingOps = releaseMonitor();
                assert( pendingOps == 1 );
            }
            else
            {
                /* SLT: --- Starter1 -- Stopper --- */
                int interestOps = m_selectionKey.interestOps();
                m_selectionKey.cancel();
                m_selectionKey = null;

                if ((interestOps & SelectionKey.OP_ACCEPT) != 0)
                    m_collider.executeInThreadPool( new Stopper2() );
                /* else Starter3 will be executed soon. */
            }
        }
    }

    private class Stopper2 extends ThreadPool.Runnable
    {
        public void runInThreadPool()
        {
            int pendingOps = releaseMonitor();
            assert( pendingOps > 0 );
            closeChannel();
        }
    }

    private void closeChannel()
    {
        try
        {
            m_channel.close();
        }
        catch (IOException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning( ex.toString() );
        }
        m_channel = null;
    }

    private static final Logger s_logger = Logger.getLogger( AcceptorImpl.class.getName() );

    private final Acceptor m_acceptor;
    private Selector m_selector;
    private ServerSocketChannel m_channel;
    private SelectionKey m_selectionKey;
    private final AtomicInteger m_stop;
    private final ChannelAcceptor m_channelAcceptor;
    private final Starter3 m_starter3;

    public AcceptorImpl(
            ColliderImpl collider,
            Acceptor acceptor,
            Selector selector,
            InputQueue.DataBlockCache inputQueueDataBlockCache,
            OutputQueue.DataBlockCache outputQueueDataBlockCache,
            ServerSocketChannel channel )
    {
        super( collider, acceptor, inputQueueDataBlockCache, outputQueueDataBlockCache );

        m_selector = selector;
        m_acceptor = acceptor;
        m_channel = channel;
        m_stop = new AtomicInteger(0);
        m_channelAcceptor = new ChannelAcceptor();
        m_starter3 = new Starter3();
    }

    public final void start()
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( m_channel.socket().getLocalSocketAddress().toString() );

        m_collider.executeInSelectorThread( new Starter1() );
    }

    public void stopAndWait() throws InterruptedException
    {
        if (s_logger.isLoggable(Level.FINE))
            s_logger.fine( m_channel.socket().getLocalSocketAddress().toString() );

        int stop = m_stop.getAndIncrement();
        if (stop == 0)
            m_collider.executeInSelectorThread( new Stopper1() );

        waitMonitor();
    }

    public void handleReadyOps( ThreadPool threadPool )
    {
        m_selectionKey.interestOps( 0 );
        threadPool.execute( m_channelAcceptor );
    }
}
