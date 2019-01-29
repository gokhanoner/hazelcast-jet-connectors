/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oner.connectors.util;

import com.hazelcast.jet.impl.util.ExceptionUtil;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Utility class for making simple Http calls.
 */
public final class SimpleHttpClient {
    private static final Logger LOGGER = Logger.getLogger(SimpleHttpClient.class.getSimpleName());

    private static final int HTTP_OK = 200;

    private final String url;
    private final Map<String, String> headers = new LinkedHashMap<>(1);
    private final Map<String, String> params = new LinkedHashMap<>(1);
    private String body;

    private SimpleHttpClient(String url) {
        this.url = url;
    }

    public static SimpleHttpClient create(String url) {
        return new SimpleHttpClient(url);
    }

    public SimpleHttpClient withHeader(String key, String value) {
        headers.put(key, value);
        return this;
    }

    public SimpleHttpClient withParam(String key, String value) {
        params.put(key, value);
        return this;
    }

    public SimpleHttpClient withBody(String body) {
        this.body = body;
        return this;
    }

    public String get() {
        return call("GET", getParamsString(params));
    }

    public String postWithBody() {
        return call("POST", body);
    }

    public String postWithParams() {
        return call("POST", getParamsString(params));
    }

    private String call(String method, String body) {
        HttpURLConnection connection = null;
        DataOutputStream outputStream = null;
        try {
            URL urlToConnect = body == null || body.isEmpty()
                    ? new URL(url)
                    : new URL(String.format("%s?%s", url, body));
            connection = (HttpURLConnection) urlToConnect.openConnection();
            connection.setRequestMethod(method);
            for (Map.Entry<String, String> header : headers.entrySet()) {
                connection.setRequestProperty(header.getKey(), header.getValue());
            }
            if ("POST".equals(method) && body != null) {
                byte[] bodyData = body.getBytes(StandardCharsets.UTF_8);

                connection.setDoOutput(true);
                connection.setRequestProperty("charset", "utf-8");
                connection.setRequestProperty("Content-Length", Integer.toString(bodyData.length));

                outputStream = new DataOutputStream(connection.getOutputStream());
                outputStream.write(bodyData);
                outputStream.flush();
            }

            if (connection.getResponseCode() != HTTP_OK) {
                throw new IllegalStateException(String.format("Failure executing: %s at: %s. Message: %s,", method, url,
                        read(connection.getErrorStream())));
            }
            return read(connection.getInputStream());
        } catch (Exception e) {
            throw new IllegalStateException("Failure in executing REST call", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
            if (outputStream != null) {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    LOGGER.log(Level.FINEST, "Error while closing HTTP output stream", e);
                }
            }
        }
    }

    private String read(InputStream stream) {
        if (stream == null) {
            return "";
        }
        Scanner scanner = new Scanner(stream, "UTF-8");
        scanner.useDelimiter("\\Z");
        return scanner.next();
    }

    private String getParamsString(Map<String, String> params) {
        return params.entrySet().stream()
                .map(this::entryToString)
                .collect(Collectors.joining("&"));
    }

    private String entryToString(Map.Entry<String, String> entry) {
        try {
            return URLEncoder.encode(entry.getKey(), "UTF-8") + "=" + URLEncoder.encode(entry.getValue(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw ExceptionUtil.sneakyThrow(e);
        }
    }

}
