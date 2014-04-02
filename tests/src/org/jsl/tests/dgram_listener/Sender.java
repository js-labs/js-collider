/*
 * JS-Collider framework tests.
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

package org.jsl.tests.dgram_listener;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.Random;

public class Sender extends Thread
{
    private final int m_messageLength;
    private final DatagramChannel[] m_channels;
    private final Random m_random;
    private volatile boolean m_run;

    public Sender( int messageLength, DatagramChannel [] channels )
    {
        m_messageLength = messageLength;
        m_channels = channels;
        m_random = new Random( System.nanoTime() );
        m_run = true;
    }

    public void run()
    {
        final ByteBuffer buf = ByteBuffer.allocateDirect( m_messageLength );
        buf.putInt( 0, m_messageLength );
        for (int idx=4; idx<m_messageLength; idx++)
            buf.put( idx, (byte) idx );

        try
        {
            //final int portNumber = ((InetSocketAddress) m_channel.getLocalAddress()).getPort();
            //final InetSocketAddress target = new InetSocketAddress( "localhost", portNumber );
            int batchMsgs = 0;
            int msgs = 0;

            while (m_run)
            {
                buf.putInt( 4, ++msgs );
                for (DatagramChannel channel : m_channels)
                {
                    final int bytesSent = channel.write( buf );
                    assert (bytesSent == m_messageLength);
                    buf.position( 0 );
                }

                if (batchMsgs > 0)
                {
                    batchMsgs--;
                }
                else
                {
                    final int random = m_random.nextInt( 100 );
                    if (random < 20)
                    {
                        batchMsgs = m_random.nextInt( 1000 );
                        System.out.println(
                                "Batch: [" + msgs + ":" + (msgs+batchMsgs) + ", " + batchMsgs + "]" );
                    }
                    else
                        Thread.sleep( 100 );
                }
            }
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
        catch (InterruptedException ex)
        {
            ex.printStackTrace();
        }
    }

    public void stopAndWait()
    {
        m_run = false;
        try
        {
            join();
        }
        catch (InterruptedException ex)
        {
            ex.printStackTrace();
        }
    }
}
