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

package org.jsl.tests.session_latency;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;


public class NettyTest extends Test
{
    public class ServerHandler extends ChannelInboundHandlerAdapter
    {
        public void messageReceived(ChannelHandlerContext ctx, MessageList<Object> msgs) throws Exception
        {
            ByteBuf bb = (ByteBuf) msgs.get(0);
            ctx.write( bb );
        }

        public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause )
        {
            cause.printStackTrace();
            ctx.close();
        }
    }

    public NettyTest( int sessions, int messages, int messageLength )
    {
        super( sessions, messages, messageLength );
    }

    public void runTest()
    {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast( new ServerHandler() );
                        }
                    }).option(ChannelOption.TCP_NODELAY, true);

            // Start the server.
            ChannelFuture f = b.bind(666).sync();
            Thread.sleep(1000);
            Client client = new Client( 666, m_sessions, m_messages, m_messageLength );
            client.start();
            f.channel().closeFuture().sync();
        }
        catch (InterruptedException ex)
        {
            ex.printStackTrace();
        }
        finally
        {
            // Shut down all event loops to terminate all threads.
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
