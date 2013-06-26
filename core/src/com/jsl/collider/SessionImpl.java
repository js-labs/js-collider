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
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;


public class SessionImpl implements Collider.ChannelHandler, Session
{
    private class SelectorRegistrator extends Collider.SelectorThreadRunnable
    {
        private Selector m_selector;

        SelectorRegistrator( Selector selector )
        {
            m_selector = selector;
        }

        public void runInSelectorThread()
        {
            try
            {
                m_selectionKey = m_socketChannel.register( m_selector, 0 );
            }
            catch (IOException ex)
            {
                System.out.println( ex.toString() );
                m_waitingMonitor.release();
                m_waitingMonitor = null;
                return;
            }
            m_collider.executeInThreadPool( new Initializer() );
        }
    }

    private class Initializer implements Runnable
    {
        public void run()
        {
            m_inputQueue.initialize( m_selectionKey );
            m_waitingMonitor.release();
            m_waitingMonitor = null;
        }
    }

    private Collider m_collider;
    private Monitor m_waitingMonitor;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private InputQueue m_inputQueue;
    private OutputQueue m_outputQueue;

    public SessionImpl(
            Collider collider,
            Selector selector,
            Monitor waitingMonitor,
            SocketChannel socketChannel,
            Session.HandlerFactory handlerFactory )
    {
        m_collider = collider;
        m_waitingMonitor = waitingMonitor;
        m_socketChannel = socketChannel;
        m_inputQueue = new InputQueue( collider, socketChannel, handlerFactory.createHandler(this) );
        m_outputQueue = new OutputQueue();

        waitingMonitor.acquire();
        collider.executeInSelectorThread( new SelectorRegistrator(selector) );
    }

    public void handleReadyOps( Executor executor )
    {
        int readyOps = m_selectionKey.readyOps();
        m_selectionKey.interestOps( m_selectionKey.interestOps() & ~readyOps );

        if ((readyOps & SelectionKey.OP_READ) != 0)
            executor.execute( m_inputQueue );

        if ((readyOps & SelectionKey.OP_WRITE) != 0)
            executor.execute( m_outputQueue );
    }

    public Collider getCollider() { return m_collider; }
    public SocketAddress getLocalSocketAddress() { return m_socketChannel.socket().getLocalSocketAddress(); }
    public SocketAddress getRemoteSocketAddress() { return m_socketChannel.socket().getRemoteSocketAddress(); }

    public int closeConnection()
    {
        return 0;
    }

    public int sendData( ByteBuffer data )
    {
        return 0;
    }
}
