/*
 * JS-Collider framework.
 * Copyright (C) 2013 Sergey Zubarev
 * java.socket.collider@gmail.com
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.jsl.collider;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.*;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;


public class AcceptorImpl implements Acceptor, Runnable, Collider.ChannelHandler
{
    private class SelectorRegistrator extends Collider.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            try
            {
                m_selectionKey = m_channel.register(
                        m_selector, SelectionKey.OP_ACCEPT, AcceptorImpl.this );
            }
            catch (IOException ex)
            {
                System.out.println( ex.toString() );
            }
        }
    }

    private class Stopper extends Collider.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (m_selectionKey == null)
            {
                m_monitor.release();
            }
            else
            {
                int interestOps = m_selectionKey.interestOps();

                m_selectionKey.cancel();
                m_selectionKey = null;
                try
                {
                    m_channel.close();
                }
                catch (IOException ex)
                {
                    System.out.println( ex.toString() );
                }
                m_channel = null;

                if ((interestOps & SelectionKey.OP_ACCEPT) != 0)
                    m_monitor.release();
            }
        }
    }

    private class InterestRegistrator extends Collider.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
            if (m_selectionKey == null)
                m_monitor.release();
            else
                m_selectionKey.interestOps( SelectionKey.OP_ACCEPT );
        }
    }

    private Collider m_collider;
    private Selector m_selector;
    private ServerSocketChannel m_channel;
    private Acceptor.HandlerFactory m_handlerFactory;
    private SelectionKey m_selectionKey;
    private InterestRegistrator m_interestRegistrator;

    private AtomicInteger m_close;
    private Monitor m_monitor;

    public AcceptorImpl(
            Collider collider,
            Selector selector,
            ServerSocketChannel channel,
            Acceptor.HandlerFactory handlerFactory )
    {
        m_collider = collider;
        m_selector = selector;
        m_channel = channel;
        m_handlerFactory = handlerFactory;
        m_interestRegistrator = new InterestRegistrator();

        m_close = new AtomicInteger();
        m_monitor = new Monitor(1);

        collider.executeInSelectorThread( new SelectorRegistrator() );
    }

    public void handleReadyOps( Executor executor )
    {
        int readyOps = m_selectionKey.readyOps();
        if ((readyOps & SelectionKey.OP_ACCEPT) != 0)
        {
            m_selectionKey.interestOps( 0 );
            executor.execute( this );
        }
        else
            System.out.println( "Internal error: invalid readyOps=" + readyOps + "." );
    }

    public void close()
    {
        if (m_close.incrementAndGet() == 1)
            m_collider.executeInSelectorThread( new Stopper() );
        m_monitor.await();
    }

    public void run()
    {
        while (m_close.get() == 0)
        {
            try
            {
                SocketChannel socketChannel = m_channel.accept();
                if (socketChannel == null)
                    break;
                socketChannel.configureBlocking( false );
                socketChannel.setOption( StandardSocketOptions.TCP_NODELAY, m_handlerFactory.tcpNoDelay );

                new SessionImpl( m_collider, m_selector, m_monitor, socketChannel, m_handlerFactory );
            }
            catch (IOException ex)
            {
                System.out.println( ex.toString() );
            }
        }
        m_collider.executeInSelectorThread( m_interestRegistrator );
    }
}
