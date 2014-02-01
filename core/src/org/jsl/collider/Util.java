/*
 * Copyright (C) 2013 Sergey Zubarev, info@js-labs.org
 *
 * This file is a part of JS-Collider framework.
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

package org.jsl.collider;

public class Util
{
    public static String formatDelay( long startTime, long endTime )
    {
        long delay = (endTime - startTime);
        delay /= 1000;
        if (delay == 0)
            return "0.0";
        long sec = (delay / 1000000);
        long usec = (delay % 1000000);
        return String.format( "%d.%06d", sec, usec );
    }
}
