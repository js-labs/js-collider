/*
 * JS-Collider framework.
 * Copyright (C) 2013 Sergey Zubarev
 * info@js-labs.org
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

package org.jsl.tests.output_queue;

import org.jsl.collider.OutputQueue;
import org.jsl.collider.PacketHandler;
import org.jsl.tests.Util;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;


public class Main
{
    private static final int MESSAGE_MAGIC = 0x1A2B3C4D;
    private Semaphore m_sema;
    private AtomicLong m_state;
    private OutputQueue m_outputQueue;
    private Handler m_handler;
    private int m_waitMessages;
    private int m_messages;

    private class Handler extends PacketHandler
    {
        public Handler()
        {
            super( 4 );
        }

        protected int onValidateHeader( ByteBuffer header )
        {
            return header.getInt();
        }

        protected void onPacketReceived( ByteBuffer packet )
        {
            packet.getInt(); // skip length
            int magic = packet.getInt();
            assert( magic == MESSAGE_MAGIC );
            m_messages++;
        }

        public void onConnectionClosed() {}
    }

    private Main()
    {
        m_sema = new Semaphore(0);
        m_state = new AtomicLong(0);
        m_outputQueue = new OutputQueue( false, 1000 );
        m_handler = new Handler();
        m_waitMessages = 0;
        m_messages = 0;
    }

    private void startGenerator( int messages, int messageSize )
    {
        new Generator(this, messages, messageSize, MESSAGE_MAGIC).start();
        m_waitMessages += messages;
    }

    private void run()
    {
        final int MESSAGES = 100000;

        this.startGenerator( MESSAGES, 5000 );
        this.startGenerator( MESSAGES, 3280 );
        this.startGenerator( MESSAGES, 128 );
        this.startGenerator( MESSAGES, 1000 );
        this.startGenerator( MESSAGES, 300 );
        this.startGenerator( MESSAGES, 510 );
        this.startGenerator( MESSAGES, 4576 );

        ByteBuffer [] iov = new ByteBuffer[4];
        long bytesProcessed = 0;
        int waits = 0;

        long startTime = System.nanoTime();
        while (m_messages < m_waitMessages)
        {
            try { m_sema.acquire(); } catch (InterruptedException ignored) {}
            long state = m_state.get();
            assert( state > 0 );
            while (state > 0)
            {
                long bytesReady = m_outputQueue.getData( iov, state );
                int pos0 = iov[0].position();
                for (int idx=0; idx<iov.length && iov[idx]!=null; idx++)
                    m_handler.onDataReceived( iov[idx] );
                for (int idx=0; idx<iov.length; idx++)
                    iov[idx] = null;
                m_outputQueue.removeData( pos0, bytesReady );
                bytesProcessed += bytesReady;
                state = m_state.addAndGet( -bytesReady );
            }
            waits++;
        }
        long endTime = System.nanoTime();
        System.out.println( m_messages + " messages processed (" + bytesProcessed
                            + " bytes) processed at " + Util.formatDelay(startTime, endTime)
                            + ", " + waits + " waits." );
    }

    public void addData( ByteBuffer data )
    {
        long bytesReady = m_outputQueue.addData( data );
        if (bytesReady > 0)
        {
            long state = m_state.addAndGet( bytesReady );
            if (state == bytesReady)
                m_sema.release();
        }
    }

    public static void main(String[] args)
    {
        new Main().run();
    }
}
