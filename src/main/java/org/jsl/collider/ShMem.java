/*
 * Copyright (C) 2013 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of JS-Collider framework.
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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.logging.Level;
import java.util.logging.Logger;


public abstract class ShMem
{
    protected static final Logger s_logger = Logger.getLogger( "org.jsl.collider.ShMem" );

    public static class Channel
    {
        protected final File m_file;
        protected final int m_blockSize;
        protected final int m_nextBlockPos;
        protected final FileChannel m_fileChannel;
        protected MappedByteBuffer [] m_mbb;

        public Channel( File file, int blockSize, boolean init ) throws IOException
        {
            m_file = file;
            m_blockSize = blockSize;
            m_nextBlockPos = (blockSize - 4);
            m_fileChannel = new RandomAccessFile(file, "rw").getChannel();
            m_mbb = new MappedByteBuffer[8];

            for (int idx=0; idx<2; idx++)
            {
                m_mbb[idx] = m_fileChannel.map( FileChannel.MapMode.READ_WRITE, idx*m_blockSize, m_blockSize );
                if (init)
                {
                    m_mbb[idx].putInt( m_nextBlockPos, -1 );

                    /* It is better to set all possible permission to the file
                     * for a case if server runs under different user.
                      */
                    if (!file.setReadable(true, false))
                    {
                        if (s_logger.isLoggable(Level.WARNING))
                            s_logger.warning( "File.setReadable('" + file.getAbsolutePath() + "') failed." );
                    }

                    if (!file.setWritable(true, false))
                    {
                        if (s_logger.isLoggable(Level.WARNING))
                            s_logger.warning( "File.setWritable('" + file.getAbsolutePath() + "') failed." );
                    }
                }
            }
        }

        public final int getBlockSize()
        {
            return m_blockSize;
        }

        public final File getFile()
        {
            return m_file;
        }

        public void close()
        {
            try
            {
                m_fileChannel.close();
            }
            catch (IOException ex)
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( ex.toString() );
            }

            if (!m_file.delete())
            {
                if (s_logger.isLoggable(Level.WARNING))
                    s_logger.warning( "File.delete('" + m_file.getAbsolutePath() + "') failed." );
            }
        }
    }

    public static class ChannelIn extends Channel
    {
        private int m_idx;

        public ChannelIn( File file, int blockSize, boolean init ) throws IOException
        {
            super( file, blockSize, init );
            m_idx = 0;
        }

        public final int handleData( int size, Session.Listener listener )
        {
            for (;;)
            {
                final MappedByteBuffer buf = m_mbb[m_idx];
                assert( buf.capacity() == m_blockSize );
                int pos = buf.position();
                final int blockBytes = (m_nextBlockPos - pos);
                if (size <= blockBytes)
                {
                    pos += size;
                    buf.limit( pos );
                    //FIXME listener.onDataReceived( buf );
                    buf.position( pos );
                    return 0;
                }

                buf.limit( m_nextBlockPos );
                //FIXME listener.onDataReceived( buf );

                buf.clear();
                final int nextIdx = buf.getInt( m_nextBlockPos );
                buf.putInt( m_nextBlockPos, -1 );
                size -= blockBytes;

                if ((nextIdx >= m_mbb.length) || (m_mbb[nextIdx] == null))
                {
                    /* The block is not mapped yet. */
                    if (nextIdx >= m_mbb.length)
                    {
                        MappedByteBuffer [] mbb = new MappedByteBuffer[m_mbb.length*2];
                        System.arraycopy( m_mbb, 0, mbb, 0, m_mbb.length );
                        m_mbb = mbb;
                    }

                    MappedByteBuffer nextBuf;
                    try
                    {
                        nextBuf = m_fileChannel.map(
                                FileChannel.MapMode.READ_WRITE, nextIdx*m_blockSize, m_blockSize );
                    }
                    catch (IOException ex)
                    {
                        if (s_logger.isLoggable(Level.WARNING))
                        {
                            s_logger.warning(
                                    "FileChannel.map(" + m_file.getAbsolutePath() + ", " +
                                    nextIdx*m_blockSize + ", " + m_blockSize + "') failed." );
                        }
                        /* Most probably error is not recoverable.
                         * Let's return -1 notifying caller to close connection.
                         */
                        return -1;
                    }

                    m_mbb[nextIdx] = nextBuf;
                }

                m_idx = nextIdx;
            }
        }
    }

    public static class ChannelOut extends Channel
    {
        private int m_idx;

        public ChannelOut( File file, int blockSize, boolean init ) throws IOException
        {
            super( file, blockSize, init );
            m_idx = 0;
        }

        public final int addData( ByteBuffer data )
        {
            final int dataPosition = data.position();
            final int dataLimit = data.limit();

            int bytesRemaining = (dataLimit - dataPosition);
            int pos = dataPosition;

            MappedByteBuffer buf = m_mbb[m_idx];

            copyLoop: for (;;)
            {
                final int space = (buf.remaining() - 4);
                if (bytesRemaining <= space)
                {
                    buf.put( data );
                    return (dataLimit - dataPosition);
                }

                if (space > 0)
                {
                    pos += space;
                    bytesRemaining -= space;
                    data.limit( pos );
                    buf.put( data );
                    data.limit( dataLimit );
                }

                int idx = 0;
                for (; idx<m_mbb.length; idx++)
                {
                    if (idx == m_idx)
                        continue;

                    final MappedByteBuffer nextBuf = m_mbb[idx];
                    if (nextBuf == null)
                        break;

                    assert( nextBuf.capacity() == m_blockSize );
                    if (nextBuf.getInt(m_nextBlockPos) == -1)
                    {
                        buf.putInt( m_nextBlockPos, idx );
                        buf.clear();
                        buf = nextBuf;
                        m_idx = idx;
                        continue copyLoop;
                    }
                }

                if (idx == m_mbb.length)
                {
                    MappedByteBuffer [] mbb = new MappedByteBuffer[m_mbb.length*2];
                    System.arraycopy( m_mbb, 0, mbb, 0, m_mbb.length );
                    m_mbb = mbb;
                }

                MappedByteBuffer nextBuf;
                try
                {
                    nextBuf = m_fileChannel.map(
                            FileChannel.MapMode.READ_WRITE, idx*m_blockSize, m_blockSize );
                }
                catch (IOException ex)
                {
                    if (s_logger.isLoggable(Level.WARNING))
                    {
                        s_logger.warning(
                                "FileChannel.map(" + m_file.getAbsolutePath() + ", " +
                                idx*m_blockSize + ", " + m_blockSize + "') failed." );
                    }
                    /* Most probably error is not recoverable.
                     * Let's return -1 notifying caller to close connection.
                     */
                    return -1;
                }

                buf.putInt( m_nextBlockPos, idx );
                buf.clear();
                buf = nextBuf;
                m_mbb[idx] = buf;
                m_idx = idx;
            }
        }
    }

    public String toString()
    {
        return getIn().getBlockSize() + ";" +
               getIn().getFile().getAbsolutePath() + ";" +
               getOut().getFile().getAbsolutePath();
    }

    abstract ChannelIn getIn();
    abstract ChannelOut getOut();
    abstract void close();
}
