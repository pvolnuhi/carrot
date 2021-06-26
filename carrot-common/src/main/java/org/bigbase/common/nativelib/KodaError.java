/**
 *    Copyright (C) 2021-present Carrot, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 */

package org.bigbase.common.nativelib;


/**
 * Used when serious errors (unchecked exception) in LibLoader are observed.
 *  
 */

public class KodaError extends Error
{
    /**
     * 
     */
    private static final long    serialVersionUID = 1L;

    public final KodaErrorCode errorCode;

    public KodaError(KodaErrorCode code) {
        super();
        this.errorCode = code;
    }

    public KodaError(KodaErrorCode code, Error e) {
        super(e);
        this.errorCode = code;
    }

    public KodaError(KodaErrorCode code, String message) {
        super(message);
        this.errorCode = code;
    }

    @Override
    public String getMessage() {
        return String.format("[%s] %s", errorCode.name(), super.getMessage());
    }

}

