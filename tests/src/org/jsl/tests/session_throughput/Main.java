package org.jsl.tests.session_throughput;

public class Main
{
    public static void main( String [] args )
    {
        int sessions = 1;
        int messages = 1000000;
        int messageLength = 100;
        int socketRecvBufSize = (256*1024);

        if (args.length > 0)
            sessions = Integer.parseInt( args[0] );
        if (args.length > 1)
            messages = Integer.parseInt( args[1] );
        if (args.length > 2)
            messageLength = Integer.parseInt( args[2] );

        Client client = new Client( sessions, messages, messageLength );
        new Server(client, socketRecvBufSize).run();
    }
}
