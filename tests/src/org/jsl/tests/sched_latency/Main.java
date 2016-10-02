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

public class Main
{
    public static void print_res( String str, long [] res, int skip )
    {
        long min = res[skip];
        long max = res[skip];
        long sum = res[skip];

        for (int idx=skip; idx<res.length; idx++)
        {
            long tt = res[idx];
            if (tt < min)
                min = tt;
            if (tt > max)
                max = tt;
            sum += tt;
            tt /= 1000;
            if (idx < (skip+20))
                System.out.print( tt + " " );
        }
        System.out.println();

        min /= 1000;
        max /= 1000;
        sum /= 1000;
        long avg = (sum / res.length);

        System.out.println( str + ": min=" + min + " max=" + max + " avg=" + avg );
    }

    public static void main( String args [] )
    {
        final int OPS = 100;
        final int SKIP = 10;
        long res [] = new long[OPS];

        new SL_Semaphore(res).start();
        print_res( "Semaphore", res, SKIP );

        new SL_Cond(res).start();
        print_res( "Cond", res, SKIP );

        new SL_Executor(res).start();
        print_res( "Executor", res, SKIP );

        new SL_ThreadPool(res).start();
        print_res( "ThreadPool", res, SKIP );
    }
}
