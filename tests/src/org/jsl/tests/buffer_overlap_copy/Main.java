/*
 * JS-Collider framework test.
 * Copyright (C) 2013 Sergey Zubarev
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

package org.jsl.tests.buffer_overlap_copy;

import java.nio.ByteBuffer;

public class Main
{
    final static int COUNT = 10;

    private static void testBuffer( final ByteBuffer bb ) throws Exception
    {
        final ByteBuffer dd = bb.duplicate();

        for (int idx=0; idx<COUNT; idx++)
            bb.putInt( idx );
        dd.position( 4*3 );
        bb.position(0);
        bb.put( dd );
        for (int idx=0; idx<7; idx++)
        {
            int v = bb.getInt( idx*4 );
            if (v != (idx+3))
                throw new Exception("bad");
        }
    }

    public static void main( String [] args ) throws Exception
    {
        testBuffer( ByteBuffer.allocate(4*COUNT) );
        testBuffer( ByteBuffer.allocateDirect(4*COUNT) );
    }
}
