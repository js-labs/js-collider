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

import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class LogFormatter extends Formatter
{
    public String format( LogRecord logRecord )
    {
        StringBuilder sb = new StringBuilder();

        Date date = new Date( logRecord.getMillis() );
        sb.append( date.toString() );
        sb.append( " " );

        sb.append( "t@" );
        sb.append( logRecord.getThreadID() );
        sb.append( " " );

        sb.append( logRecord.getSourceClassName() );
        sb.append( "." );
        sb.append( logRecord.getSourceMethodName() );
        sb.append( " " );

        sb.append( logRecord.getMessage() );
        sb.append( "\n" );

        return sb.toString();
    }
}
