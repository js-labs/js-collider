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

package org.jsl.tests.sched_latency;

import java.util.Arrays;

public class Main
{
    public static void printResult(String str, long [] res)
    {
        final int c = Math.min(20, res.length);
        for (int idx=0; idx<c; idx++)
            System.out.print(res[idx]/1000 + " ");
        System.out.println();

        Arrays.sort(res);
        final long min = res[0] / 1000;
        final long med = res[res.length/2] / 1000;
        final long max = res[res.length-1] / 1000;
        System.out.println(str + ": min=" + min + " med=" + med + " max=" + max);
    }

    public static void main(String args [])
    {
        final int OPS = 100;
        final long res [] = new long[OPS];

        new SL_Semaphore(res).start();
        printResult("Semaphore", res);

        new SL_Cond(res).start();
        printResult("Cond", res);

        new SL_Executor(res).start();
        printResult("Executor", res);

        new SL_ThreadPool(res).start();
        printResult("ThreadPool", res);
    }
}
