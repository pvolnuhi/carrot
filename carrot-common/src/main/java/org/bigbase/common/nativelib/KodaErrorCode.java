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
 * Error codes of snappy-java
 * 
 * 
 */
public enum KodaErrorCode {

    // DO NOT change these error code IDs because these numbers are used inside SnappyNative.cpp
    UNKNOWN(0),
    FAILED_TO_LOAD_NATIVE_LIBRARY(1),
    PARSING_ERROR(2),
    NOT_A_DIRECT_BUFFER(3),
    OUT_OF_MEMORY(4),
    FAILED_TO_UNCOMPRESS(5);

    public final int id;

    private KodaErrorCode(int id) {
        this.id = id;
    }

    public static KodaErrorCode getErrorCode(int id) {
        for (KodaErrorCode code : KodaErrorCode.values()) {
            if (code.id == id)
                return code;
        }
        return UNKNOWN;
    }

    public static String getErrorMessage(int id) {
        return getErrorCode(id).name();
    }
}

