/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package main.java;

import com.couchbase.client.core.env.DefaultCoreEnvironment;

import java.io.Serializable;

/**
 * @author Sergey Avseyev
 * @author David Ostrovsky
 */
public class DefaultDcpConsumerEnvironment extends DefaultCoreEnvironment implements DcpConsumerEnvironment, Serializable{
    public static DefaultDcpConsumerEnvironment create() {
        return new DefaultDcpConsumerEnvironment(builder());
    }

    public static Builder builder() {
        return new Builder();
    }


    protected DefaultDcpConsumerEnvironment(final Builder builder) {
        super(builder);

        if (!dcpEnabled()) {
            throw new IllegalStateException("DCP integration cannot work without DCP enabled.");
        }
    }

    public static class Builder extends DefaultCoreEnvironment.Builder implements DcpConsumerEnvironment {
        @Override
        public DefaultDcpConsumerEnvironment build() {
            return new DefaultDcpConsumerEnvironment(this);
        }
    }
}
