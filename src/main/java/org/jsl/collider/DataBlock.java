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

public class DataBlock
{
    public DataBlock next;
    public final ByteBuffer ww;
    public final ByteBuffer rw;

    public DataBlock( ByteBuffer buf )
    {
        ww = buf;
        rw = buf.duplicate();
    }

    public final DataBlock reset()
    {
        next = null;
        ww.clear();
        rw.clear();
        return this;
    }
}
