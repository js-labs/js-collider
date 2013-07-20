/*
 * JS-Collider framework.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.com
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
import java.nio.ByteBuffer;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;


public class InputQueue extends Collider.SelectorThreadRunnable implements Runnable
{
    private static class DataBlock
    {
        public DataBlock next;
        public ByteBuffer byteBuffer;
        public ByteBuffer rw;
        public ByteBuffer ww;

        public DataBlock( int size )
        {
            next = null;
            byteBuffer = ByteBuffer.allocateDirect( size );
            rw = byteBuffer.duplicate();
            ww = byteBuffer.duplicate();
        }
    }

    private static final ThreadLocal<DataBlock> s_tlsDataBlock = new ThreadLocal<DataBlock>();

    private static int BLOCK_SIZE = (1024 * 16);

    private Collider m_collider;
    private SocketChannel m_socketChannel;
    private SelectionKey m_selectionKey;
    private Session.Handler m_handler;

    private final int LENGTH_MASK = 0x3FFFFFFF;
    private final int CLOSED      = 0x40000000;
    private AtomicInteger m_length;
    private AtomicReference<DataBlock> m_dataBlock;

    private void abort()
    {
        System.out.println( Thread.currentThread().getStackTrace().toString() );
        System.exit( -1 );
    }

    private void readAndHandleData()
    {
        DataBlock dataBlock = s_tlsDataBlock.get();
        if (dataBlock == null)
            dataBlock = new DataBlock( BLOCK_SIZE );
        else
            s_tlsDataBlock.remove();

        int bytesReceived = this.readData( dataBlock );
        if (bytesReceived > 0)
        {
            m_dataBlock.set( dataBlock );
            m_length.set( bytesReceived );
            m_collider.executeInSelectorThread( this );
            this.handleData( dataBlock, bytesReceived );
        }
        else
        {
            m_handler.onConnectionClosed();
            s_tlsDataBlock.set( dataBlock );
        }
    }

    private int readData( DataBlock dataBlock )
    {
        try
        {
            int bytesReceived = m_socketChannel.read( dataBlock.ww );
            if (bytesReceived > 0)
                return bytesReceived;
        }
        catch (NotYetConnectedException ignored) { }
        catch (IOException ignored) { }
        return -1;
    }

    private void handleData( DataBlock dataBlock, int bytesReady )
    {
        int length;
        ByteBuffer rw = dataBlock.rw;
        int limit = rw.limit();
        for (;;)
        {
            limit += bytesReady;
            if (limit > rw.capacity())
                abort();
            rw.limit( limit );

            m_handler.onDataReceived( rw );
            rw.position( limit );

            length = m_length.addAndGet( -bytesReady );
            int bytesRest = (length & LENGTH_MASK);
            if (bytesRest == 0)
                break;

            if (rw.capacity() == rw.position())
            {
                DataBlock db = dataBlock.next;
                dataBlock.next = null;
                dataBlock.rw.clear();
                dataBlock.ww.clear();
                if (s_tlsDataBlock.get() == null)
                    s_tlsDataBlock.set( dataBlock );
                dataBlock = db;
                rw = dataBlock.rw;
                limit = 0;
            }
        }

        if ((length & CLOSED) == 0)
            m_handler.onConnectionClosed();

        if (m_dataBlock.compareAndSet(dataBlock, null))
        {
            if (s_tlsDataBlock.get() == null)
                s_tlsDataBlock.set( dataBlock );
        }
    }

    public InputQueue(
            Collider collider,
            SocketChannel socketChannel,
            Session.Handler handler )
    {
        m_collider = collider;
        m_socketChannel = socketChannel;
        m_handler = handler;
        m_length = new AtomicInteger();
        m_dataBlock = new AtomicReference<DataBlock>();

        m_collider.executeInSelectorThread( this );
    }

    public void runInSelectorThread()
    {
        int interestOps = m_selectionKey.interestOps();
        assert( (interestOps & SelectionKey.OP_READ) == 0 );
        m_selectionKey.interestOps( interestOps | SelectionKey.OP_READ );
    }

    public void run()
    {
        DataBlock dataBlock = m_dataBlock.get();

        int length = m_length.get();
        if (length == 0)
        {
            this.readAndHandleData();
            return;
        }

        DataBlock prev = null;
        if (dataBlock.ww.remaining() == 0)
        {
            prev = dataBlock;
            dataBlock = s_tlsDataBlock.get();
            if (dataBlock == null)
                dataBlock = new DataBlock( BLOCK_SIZE );
            else
                s_tlsDataBlock.remove();
        }

        int bytesReceived = this.readData( dataBlock );
        if (bytesReceived > 0)
        {
            if (prev != null)
                prev.next = dataBlock;

            for (;;)
            {
                int newLength = (length & LENGTH_MASK) + bytesReceived;
                /*
                if ((newLength & LENGTH_MASK) > LENGTH_MASK)
                    Queue is too big !!!
                */
                if (m_length.compareAndSet(length, newLength))
                {
                    length = newLength;
                    break;
                }
                length = m_length.get();
            }

            if (length == bytesReceived)
            {
                if (prev != null)
                {
                    prev.next = null;
                    if (s_tlsDataBlock.get() == null)
                        s_tlsDataBlock.set( prev );
                    m_dataBlock.set( dataBlock );
                }

                m_collider.executeInSelectorThread( this );
                this.handleData( dataBlock, bytesReceived );
            }
            else
            {
                m_collider.executeInSelectorThread( this );
            }
        }
        else
        {
            for (;;)
            {
                int newLength = (length | CLOSED);
                if (m_length.compareAndSet(length, newLength))
                {
                    length = newLength;
                    break;
                }
                length = m_length.get();
            }

            if ((length & LENGTH_MASK) == 0)
            {
                m_handler.onConnectionClosed();
                if (prev != null)
                {
                    if (s_tlsDataBlock.get() == null)
                        s_tlsDataBlock.set( dataBlock );
                }
            }
        }
    }
}
