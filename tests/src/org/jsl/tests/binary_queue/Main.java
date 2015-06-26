/*
 * JS-Collider framework.
 * Copyright (C) 2015 Sergey Zubarev
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

package org.jsl.tests.binary_queue;

import org.jsl.collider.DataBlockCache;
import org.jsl.tests.StreamDefragger;
import org.jsl.tests.BinaryQueue;
import org.jsl.tests.Util;

import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;


public class Main
{
    private static final int MESSAGE_MAGIC = 0x1A2B3C4D;
    private static final Logger s_logger = Logger.getLogger( Main.class.getName() );

    private final Semaphore m_sema;
    private final AtomicLong m_state;
    private final DataBlockCache m_dataBlockCache;
    private final BinaryQueue m_outputQueue;
    private final Stream m_stream;
    private int m_waitMessages;
    private int m_messages;

    private class Stream extends StreamDefragger
    {
        public Stream()
        {
            super( 4 );
        }

        protected int validateHeader( ByteBuffer header )
        {
            return header.getInt();
        }
    }

    private Main()
    {
        m_sema = new Semaphore(0);
        m_state = new AtomicLong(0);
        m_dataBlockCache = new DataBlockCache( false, 1000, 20, 100 );
        m_outputQueue = new BinaryQueue( m_dataBlockCache );
        m_stream = new Stream();
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
        final int MESSAGES = 10000;

        this.startGenerator( MESSAGES, 5000 );
        this.startGenerator( MESSAGES, 3280 );
        this.startGenerator( MESSAGES, 126 );
        this.startGenerator( MESSAGES, 1000 );
        this.startGenerator( MESSAGES, 300 );
        this.startGenerator( MESSAGES, 510 );
        this.startGenerator( MESSAGES, 4576 );
        this.startGenerator( MESSAGES, 777 );
        this.startGenerator( MESSAGES, 4 );

        ByteBuffer [] iov = new ByteBuffer[4];
        long bytesProcessed = 0;
        int waits = 0;

        /*
        try { Thread.sleep(4000); }
        catch (InterruptedException ignored) {}
        */

        long startTime = System.nanoTime();
        while (m_messages < m_waitMessages)
        {
            try { m_sema.acquire(); }
            catch (InterruptedException ex)
            { System.out.println(ex.toString()); }

            long state = m_state.get();
            assert( state > 0 );
            while (state > 0)
            {
                long bytesReady = m_outputQueue.getData( iov, state );
                int pos0 = iov[0].position();
                for (int idx=0; idx<iov.length && iov[idx]!=null; idx++)
                {
                    ByteBuffer msg = m_stream.getNext( iov[idx] );
                    while (msg != null)
                    {
                        final int messageLength = msg.getInt();
                        if (messageLength > 4)
                        {
                            final int magic = msg.getInt();
                            assert( magic == MESSAGE_MAGIC );
                        }
                        m_messages++;
                        msg = m_stream.getNext();
                    }
                }
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

        s_logger.setLevel( Level.FINE );
        m_dataBlockCache.clear( s_logger );
    }

    public void putData( ByteBuffer data )
    {
        final long bytesReady = m_outputQueue.putData( data );
        if (bytesReady > 0)
        {
            final long state = m_state.addAndGet( bytesReady );
            if (state == bytesReady)
                m_sema.release();
        }
    }

    public void putInt( int value )
    {
        final int bytesReady = m_outputQueue.putInt( value );
        if (bytesReady > 0)
        {
            final long state = m_state.addAndGet( bytesReady );
            if (state == bytesReady)
                m_sema.release();
        }
    }

    public static void main(String[] args)
    {
        new Main().run();
    }
}
