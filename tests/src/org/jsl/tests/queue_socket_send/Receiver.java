package org.jsl.tests.queue_socket_send;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.jsl.tests.StreamDefragger;
import org.jsl.tests.Util;


public class Receiver extends Thread
{
    private final SocketAddress m_addr;
    private final int m_socketBufferSize;

    public Receiver( SocketAddress addr, int socketBufferSize )
    {
        m_addr = addr;
        m_socketBufferSize = socketBufferSize;

    }

    public void run()
    {
        try
        {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.socket().setReceiveBufferSize( m_socketBufferSize );

            StreamDefragger streamDefragger = new StreamDefragger(4)
            {
                protected int validateHeader( ByteBuffer header )
                {
                    return header.getInt();
                }
            };

            if (!socketChannel.connect(m_addr) ||
                !socketChannel.finishConnect())
            {
                System.out.println( "SocketChannel.connect() failed." );
                return;
            }

            ByteBuffer bb = ByteBuffer.allocateDirect( m_socketBufferSize );
            int messagesReceived = 0;
            int messagesExpected = 0;
            int bytesReceivedTotal = 0;

            long startTime = System.nanoTime();
            readSocketLoop: for (;;)
            {
                int bytesReceived = socketChannel.read( bb );
                if (bytesReceived > 0)
                {
                    bb.position(0);
                    bb.limit( bytesReceived );
                    bytesReceivedTotal += bytesReceived;
                    ByteBuffer msg = streamDefragger.getNext( bb );
                    while (msg != null)
                    {
                        msg.getInt(); // skip length
                        final int mm = msg.getInt();
                        if (++messagesReceived == 1)
                            messagesExpected = mm;
                        else
                            assert( messagesExpected == mm );

                        if (messagesExpected == messagesReceived)
                            break readSocketLoop;

                        msg = streamDefragger.getNext();
                    }
                    bb.clear();
                }
            }
            long entTime = System.nanoTime();
            socketChannel.close();

            System.out.println(
                    "Received " + messagesReceived + " messages (" + bytesReceivedTotal +
                    " bytes) at " + Util.formatDelay(startTime, entTime) + "." );
        }
        catch (IOException ex)
        {
            ex.printStackTrace();
        }
    }
}
