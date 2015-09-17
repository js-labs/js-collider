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

import java.nio.ByteBuffer;

public class RetainableDataBlock
{
    /* RetainableDataBlock share retain counting
     * with aggregated RetainableByteBuffer.
     */
    public RetainableDataBlock next;
    public final ByteBuffer ww;
    public final RetainableByteBuffer rw;

    private static class BufferImpl extends RetainableByteBufferImpl
    {
        private final RetainableDataBlock m_dataBlock;

        public BufferImpl( ByteBuffer byteBuffer, RetainableDataBlock dataBlock )
        {
            super( byteBuffer );
            m_dataBlock = dataBlock;
        }

        protected void finalRelease()
        {
            super.finalRelease();
            m_dataBlock.finalRelease();
        }
    }

    protected void finalRelease()
    {
        assert( next == null );
        ww.clear();
    }

    public RetainableDataBlock( ByteBuffer byteBuffer )
    {
        ww = byteBuffer;
        rw = new BufferImpl( byteBuffer.duplicate(), this );
    }

    public final void release()
    {
        rw.release();
    }

    public final boolean clearSafe()
    {
        final boolean ret = rw.clearSafe();
        if (ret)
            ww.clear();
        return ret;
    }
}
