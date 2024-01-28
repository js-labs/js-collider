/*
 * Copyright (C) 2013 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of JS-Collider test.
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
package org.jsl.tests.unit;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.jsl.collider.DataBlock;

public class Main {
    private static void dataBlockInheritsByteOrder() throws Exception {
        final ByteOrder [] orders = {ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN};
        for (ByteOrder byteOrder: orders) {
            final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(32);
            byteBuffer.order(byteOrder);
            final DataBlock dataBlock = new DataBlock(byteBuffer);
            if (dataBlock.rd.order() != byteOrder) {
                throw new Exception("wrong dataBlock.rd.order()");
            }
            if (dataBlock.wr.order() != byteOrder) {
                throw new Exception("wrong dataBlock.wr.order()");
            }
        }
    }

    public static void main(String [] args) {
        int failedTests = 0;
        try { dataBlockInheritsByteOrder(); }
        catch (final Exception ex) {
            System.out.println(ex.getMessage());
            failedTests++;
        }
        System.out.println(failedTests + " tests failed");
    }
}
