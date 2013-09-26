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
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class Collider
{
    public static class Config
    {
        public int threadPoolThreads;
        public boolean useDirectBuffers;

        public int socketSendBufSize;
        public int socketRecvBufSize;
        public int inputQueueBlockSize;
        public int inputQueueCacheInitialSize;
        public int inputQueueCacheMaxSize;
        public int outputQueueBlockSize;
        public int outputQueueCacheInitialSize;
        public int outputQueueCacheMaxSize;

        public Config()
        {
            threadPoolThreads = 0; /* by default = number of cores */
            useDirectBuffers  = true;

            socketSendBufSize = 0; /* Use system default settings by default */
            socketRecvBufSize = 0; /* Use system default settings by default */

            inputQueueBlockSize         = (32 * 1024);
            inputQueueCacheInitialSize  = 16;
            inputQueueCacheMaxSize      = 64;
            outputQueueBlockSize        = (16 * 1024);
            outputQueueCacheInitialSize = 0;
            outputQueueCacheMaxSize     = 0; /* by default = (threadPoolThreads*3) */
        }
    }

    private static final Logger s_logger = Logger.getLogger( Collider.class.getName() );
    private final Config m_config;

    protected Collider( Config config )
    {
        m_config = config;
    }

    public final Config getConfig()
    {
        return m_config;
    }

    public abstract void run();
    public abstract void stop();

    public abstract void addAcceptor( Acceptor acceptor );
    public abstract void removeAcceptor( Acceptor acceptor ) throws InterruptedException;

    public abstract void addConnector( Connector connector );
    public abstract void removeConnector( Connector connector ) throws InterruptedException;

    public static Collider create( Config config )
    {
        Collider collider = null;
        try
        {
            collider = new ColliderImpl( config );
        }
        catch (IOException ex)
        {
            if (s_logger.isLoggable(Level.WARNING))
                s_logger.warning( ex.toString() );
        }
        return collider;
    }

    public static Collider create()
    {
        return create( new Config() );
    }
}
