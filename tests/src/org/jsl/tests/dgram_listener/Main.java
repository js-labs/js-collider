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

import org.jsl.collider.Collider;
import org.jsl.collider.DatagramListener;
import org.jsl.collider.RetainableByteBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.nio.channels.DatagramChannel;

public class Main
{
    private class Listener1 extends DatagramListener
    {
        public Listener1( InetSocketAddress addr )
        {
            super( addr );
        }

        public void onDataReceived( RetainableByteBuffer data, SocketAddress sourceAddr )
        {
            /* Should never be called. */
            assert( false );
        }
    }

    private class Listener2 extends DatagramListener
    {
        private final Collider m_collider;
        private final int m_waitPackets;
        private int m_packetsReceived;
        private int m_sequenceNumber;
        private int m_packetsLost;

        public Listener2( InetSocketAddress addr, Collider collider, int waitPackets )
        {
            super( addr );
            m_collider = collider;
            m_waitPackets = waitPackets;
        }

        public void onDataReceived( RetainableByteBuffer data, SocketAddress sourceAddr )
        {
            if (data.remaining() == 0)
                throw new RuntimeException( "zero ByteBuffer" );

            //System.out.println( Util.hexDump(data) );
            final int bytesReceived = data.remaining();
            final int messageLength = data.getInt();
            assert( bytesReceived == messageLength );

            final int packetsReceived = ++m_packetsReceived;
            assert( packetsReceived <= m_waitPackets );

            final int sequenceNumber = data.getInt();
            if (m_sequenceNumber == 0)
                m_sequenceNumber = sequenceNumber;
            else
            {
                m_sequenceNumber++;
                if (sequenceNumber != m_sequenceNumber)
                {
                    m_packetsLost++;
                    //System.out.println( "Lost packet " + m_sequenceNumber );
                    m_sequenceNumber = sequenceNumber;
                }
            }

            if (packetsReceived == m_waitPackets)
            {
                System.out.println( "Listener2: received " + m_waitPackets + " packets." );
                try
                {
                    m_collider.removeDatagramListener( this );
                }
                catch (InterruptedException ex)
                {
                    ex.printStackTrace();
                }
            }
        }
    }

    private class Listener3 extends DatagramListener
    {
        private final Collider m_collider;
        private final int m_waitPackets;
        private int m_firstReceivedSN;
        private int m_packetsReceived;
        private int m_sequenceNumber;
        private int m_packetsLost;

        public Listener3( InetSocketAddress addr, Collider collider, int waitPackets )
        {
            super( addr );
            m_collider = collider;
            m_waitPackets = waitPackets;
        }

        public void onDataReceived( RetainableByteBuffer data, SocketAddress sourceAddr )
        {
            if (data.remaining() == 0)
                throw new RuntimeException( "zero ByteBuffer" );

            //System.out.println( Util.hexDump(data) );
            final int bytesReceived = data.remaining();
            final int messageLength = data.getInt();
            assert( bytesReceived == messageLength );

            final int packetsReceived = ++m_packetsReceived;
            final int sequenceNumber = data.getInt();
            if (m_sequenceNumber == 0)
            {
                m_firstReceivedSN = sequenceNumber;
                m_sequenceNumber = sequenceNumber;
            }
            else
            {
                final int expectedSequenceNumber = ++m_sequenceNumber;
                if (sequenceNumber != expectedSequenceNumber)
                {
                    m_packetsLost += (sequenceNumber - expectedSequenceNumber);
                    //System.out.println( "Lost packet " + m_sequenceNumber );
                    m_sequenceNumber = sequenceNumber;
                }
            }

            if (packetsReceived == m_waitPackets)
            {
                final int expected = (sequenceNumber - m_firstReceivedSN);
                System.out.println( "Listener3: lost " + m_packetsLost + " of " + expected + " packets." );
                m_collider.stop();
            }
        }
    }

    private void run( int messageLength )
    {
        try
        {
            final DatagramChannel datagramChannel1 = DatagramChannel.open( StandardProtocolFamily.INET );
            datagramChannel1.socket().setReuseAddress( true );
            datagramChannel1.bind( new InetSocketAddress(0) );
            final InetSocketAddress listenAddr1 = new InetSocketAddress( "localhost", datagramChannel1.socket().getLocalPort() );
            datagramChannel1.connect( listenAddr1 );
            System.out.println( "Address [1] = " + listenAddr1 );

            final DatagramChannel datagramChannel2 = DatagramChannel.open( StandardProtocolFamily.INET );
            datagramChannel2.socket().setReuseAddress( true );
            datagramChannel2.bind( new InetSocketAddress(0) );
            final InetSocketAddress listenAddr2 = new InetSocketAddress( "localhost", datagramChannel2.socket().getLocalPort() );
            datagramChannel2.connect( listenAddr2 );
            System.out.println( "Address [2] = " + listenAddr2 );

            final Collider collider = Collider.create();
            final DatagramChannel [] channels = new DatagramChannel[] { datagramChannel1, datagramChannel2 };
            final Sender sender = new Sender( messageLength, channels );
            sender.start();

            final Listener1 listener1 = new Listener1( listenAddr1 );
            final Listener2 listener2 = new Listener2( listenAddr1, collider, 3000 );
            final Listener3 listener3 = new Listener3( listenAddr2, collider, 5000 );

            collider.addDatagramListener( listener1 );
            collider.addDatagramListener( listener2 );
            collider.addDatagramListener( listener3 );
            collider.removeDatagramListener( listener1 );
            collider.run();

            sender.stopAndWait();
            datagramChannel1.close();
            datagramChannel2.close();
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

    public static void main( String [] args )
    {
        int messageLength = 100;

        if (args.length > 0)
            messageLength = Integer.parseInt( args[0] );

        System.out.println(
                "Datagram test: " + messageLength + "." );

        new Main().run( messageLength );
    }
}
