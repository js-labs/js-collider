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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;


public class SessionImpl extends Collider.SelectorThreadRunnable
        implements Session, Collider.ChannelHandler, Runnable
{
    private static final long LENGTH_MASK   = 0x000000FFFFFFFFFFL;
    private static final long CLOSED        = 0x0000010000000000L;
    private static final long IQ_STATE_MASK = 0x0000300000000000L;
    private static final long IQ_STARTING   = 0x0000000000000000L;
    private static final long IQ_STARTED    = 0x0000100000000000L;
    private static final long IQ_STOPPED    = 0x0000200000000000L;

    private static final Logger s_logger = Logger.getLogger( SessionImpl.class.getName() );

    private Collider m_collider;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;

    private AtomicLong m_state;
    private InputQueue m_inputQueue;
    private OutputQueue m_outputQueue;
    private ByteBuffer [] m_iov;

    private static class DummyListener implements Listener
    {
        public void onDataReceived(ByteBuffer data) { }
        public void onConnectionClosed() { }
    }

    private class SelectorDeregistrator extends Collider.SelectorThreadRunnable
    {
        public void runInSelectorThread()
        {
        }
    }

    private static String stateToString( long state )
    {
        String ret = "(";
        if ((state & CLOSED) != 0)
            ret += "CLOSED ";

        long iqState = (state & IQ_STATE_MASK);
        if (iqState == IQ_STARTING)
            ret += "IQ_STARTING ";
        else if (iqState == IQ_STARTED)
            ret += "IQ_STARTED ";
        else if (iqState == IQ_STOPPED)
            ret += "IQ_STOPPED ";
        else
            ret += "UNKNOWN ";

        ret += (state & LENGTH_MASK);
        ret += ")";
        return ret;
    }

    public void onReaderStopped()
    {
        long state = m_state.get();
        for (;;)
        {
            assert( (state & IQ_STATE_MASK) != IQ_STOPPED );
            long newState = state;

            newState |= CLOSED;
            newState &= ~IQ_STATE_MASK;
            newState |= IQ_STOPPED;

            if (m_state.compareAndSet(state, newState))
                break;

            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine( m_socketChannel.socket().getRemoteSocketAddress().toString()
                    + ": onReaderStopped: " + stateToString(state) );
        }

        if ((state & LENGTH_MASK) == 0)
            m_collider.executeInSelectorThread( new SelectorDeregistrator() );
    }

    public SessionImpl(
            Collider collider,
            SessionEmitter sessionEmitter,
            SocketChannel socketChannel,
            SelectionKey selectionKey )
    {
        Collider.Config colliderConfig = collider.getConfig();

        int inputQueueBlockSize = sessionEmitter.inputQueueBlockSize;
        if (inputQueueBlockSize == 0)
            inputQueueBlockSize = colliderConfig.inputQueueBlockSize;

        int sendBufSize = sessionEmitter.socketSendBufSize;
        if (sendBufSize == 0)
            sendBufSize = colliderConfig.socketSendBufSize;
        if (sendBufSize == 0)
            sendBufSize = (64 * 1024);
        int outputQueueBlockSize = sessionEmitter.outputQueueBlockSize;
        if (outputQueueBlockSize == 0)
            outputQueueBlockSize = colliderConfig.outputQueueBlockSize;
        int sendIovMax = (sendBufSize / outputQueueBlockSize) + 1;

        m_collider = collider;
        m_socketChannel = socketChannel;
        m_selectionKey = selectionKey;
        m_state = new AtomicLong( IQ_STARTING );
        m_inputQueue = new InputQueue( collider, inputQueueBlockSize, socketChannel, selectionKey );
        m_outputQueue = new OutputQueue( colliderConfig.useDirectBuffers, outputQueueBlockSize );
        m_iov = new ByteBuffer[sendIovMax];

        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine( socketChannel.socket().getRemoteSocketAddress().toString()
                           + ": session created." );
        }
    }

    public Collider getCollider() { return m_collider; }
    public SocketAddress getLocalAddress() { return m_socketChannel.socket().getLocalSocketAddress(); }
    public SocketAddress getRemoteAddress() { return m_socketChannel.socket().getRemoteSocketAddress(); }

    public boolean sendData( ByteBuffer data )
    {
        long state = m_state.get();
        if ((state & CLOSED) != 0)
            return false;

        long bytesReady = m_outputQueue.addData( data );
        if (bytesReady > 0)
        {
            for (;;)
            {
                if ((state & CLOSED) != 0)
                    return false;

                long newState = (state & LENGTH_MASK);
                newState += bytesReady;

                if (bytesReady > LENGTH_MASK)
                {
                    state = m_state.get();
                    continue;
                }

                newState |= (state & ~LENGTH_MASK);
                if (m_state.compareAndSet(state, newState))
                {
                    state = newState;
                    break;
                }

                state = m_state.get();
            }

            if ((state & LENGTH_MASK) == bytesReady)
                m_collider.executeInThreadPool( this );
        }

        return true;
    }

    public boolean closeConnection()
    {
        long state = m_state.get();
        for (;;)
        {
            if ((state & CLOSED) != 0)
                return false;

            long newState = state;
            state |= CLOSED;

            if (m_state.compareAndSet(state, newState))
                break;

            state = m_state.get();
        }

        if (s_logger.isLoggable(Level.FINE))
        {
            s_logger.fine( m_socketChannel.socket().getRemoteSocketAddress().toString()
                           + ": closeConnection: " + stateToString(state) );
        }

        if ((state & IQ_STATE_MASK) == IQ_STARTED)
            m_inputQueue.stop();

        return true;
    }

    public void setListener( Listener listener )
    {
        if (listener == null)
            listener = new DummyListener();
        m_inputQueue.setListenerAndStart( listener );

        long state = m_state.get();
        for (;;)
        {
            if ((state & IQ_STATE_MASK) != IQ_STARTING)
                break;

            long newState = state;
            newState &= ~IQ_STATE_MASK;
            newState |= IQ_STARTED;

            if (m_state.compareAndSet(state, newState))
                break;

            state = m_state.get();
        }

        if (((state & CLOSED) != 0) &&
            ((state & IQ_STATE_MASK) == IQ_STARTING))
        {
            /* closeConnection() called */
            m_inputQueue.stop();
        }
    }

    public void handleReadyOps( Executor executor )
    {
        int readyOps = m_selectionKey.readyOps();
        m_selectionKey.interestOps( m_selectionKey.interestOps() & ~readyOps );

        if ((readyOps & SelectionKey.OP_READ) != 0)
            executor.execute( m_inputQueue );

        if ((readyOps & SelectionKey.OP_WRITE) != 0)
            executor.execute( this );
    }

    public void runInSelectorThread()
    {
        int interestOps = m_selectionKey.interestOps();
        m_selectionKey.interestOps( interestOps | SelectionKey.OP_WRITE );
    }

    public void run()
    {
        long state = m_state.get();
        long bytesReady = (state & LENGTH_MASK);
        assert( bytesReady > 0 );

        bytesReady = m_outputQueue.getData( m_iov, bytesReady );
        int pos0 = m_iov[0].position();

        int iovc = 0;
        for (; iovc<m_iov.length && m_iov[iovc]!= null; iovc++);

        try
        {
            long bytesSent = m_socketChannel.write( m_iov, 0, iovc );
            for (int idx=0; idx<iovc; idx++)
                m_iov[idx] = null;

            if (bytesSent == 0)
                System.out.println( "strange" );

            m_outputQueue.removeData( pos0, bytesSent );
            state = m_state.addAndGet( -bytesSent );

            if (bytesSent < bytesReady)
                m_collider.executeInSelectorThread( this );
            else if ((state & LENGTH_MASK) > 0)
                m_collider.executeInThreadPool( this );
            else if ((state & CLOSED) != 0)
            {
                if ((state & IQ_STATE_MASK) == IQ_STOPPED)
                    m_collider.executeInSelectorThread( new SelectorDeregistrator() );
            }
        }
        catch (IOException ex)
        {
            for (;;)
            {
                long newState = state;
                newState |= CLOSED;
                newState &= ~LENGTH_MASK;
                if (m_state.compareAndSet(state, newState))
                    break;
                state = m_state.get();
            }

            if (s_logger.isLoggable(Level.FINE))
            {
                s_logger.fine( m_socketChannel.socket().getRemoteSocketAddress().toString()
                        + ": run: exception caught:" + ex.toString() + ", " + stateToString(state) );
            }

            if ((state & IQ_STATE_MASK) == IQ_STOPPED)
                m_collider.executeInSelectorThread( new SelectorDeregistrator() );
            /* else { socket channel read listener will be notified soon } */
        }
    }
}
