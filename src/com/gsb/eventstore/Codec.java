package com.gsb.eventstore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Stable, self-delimiting text encoding for event payloads.
 *
 * <p>Only six value types are supported: String, Long, Boolean, Double, List and Map
 * (with String keys). The grammar is:
 *
 * <pre>
 *   value  := string | long | boolean | double | list | map
 *   string := 'S' &lt;char-count&gt; ':' &lt;raw chars&gt;      (length-prefixed, no escaping)
 *   long   := 'L' &lt;digits&gt; ';'
 *   bool   := 'B' ('0' | '1')
 *   double := 'D' &lt;Double.toString form&gt; ';'
 *   list   := 'A' &lt;count&gt; '{' value* '}'
 *   map    := 'M' &lt;count&gt; '{' (string value)* '}'
 * </pre>
 *
 * Strings are length-prefixed (in UTF-16 chars of the decoded text) instead of escaped,
 * so any character — including newlines, colons and braces — round-trips unambiguously.
 * The format has no version marker of its own and never changes, which keeps files
 * readable for the lifetime of the store.
 */
final class Codec {

    private Codec() {
    }

    static String encode(Object value) {
        StringBuilder out = new StringBuilder();
        encodeValue(value, out);
        return out.toString();
    }

    static Map<String, Object> decodeMap(String text) {
        Parser parser = new Parser(text);
        Object value = parser.parseValue();
        if (!(value instanceof Map)) {
            throw new IllegalStateException("Corrupt data: payload is not a map");
        }
        if (!parser.atEnd()) {
            throw new IllegalStateException("Corrupt data: trailing bytes at offset " + parser.pos);
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> map = (Map<String, Object>) value;
        return map;
    }

    private static void encodeValue(Object value, StringBuilder out) {
        if (value instanceof String) {
            String s = (String) value;
            out.append('S').append(s.length()).append(':').append(s);
        } else if (value instanceof Long) {
            out.append('L').append(value.toString()).append(';');
        } else if (value instanceof Boolean) {
            out.append('B').append(((Boolean) value).booleanValue() ? '1' : '0');
        } else if (value instanceof Double) {
            out.append('D').append(value.toString()).append(';');
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            out.append('A').append(list.size()).append('{');
            for (Object item : list) {
                encodeValue(item, out);
            }
            out.append('}');
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            out.append('M').append(map.size()).append('{');
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new IllegalArgumentException(
                            "Map keys must be Strings, got: " + entry.getKey());
                }
                encodeValue(entry.getKey(), out);
                encodeValue(entry.getValue(), out);
            }
            out.append('}');
        } else {
            throw new IllegalArgumentException("Unsupported payload value type: "
                    + (value == null ? "null" : value.getClass().getName())
                    + " (allowed: String, Long, Boolean, Double, List, Map)");
        }
    }

    /** Sequential parser over the encoded text. Package-private so the store can parse records. */
    static final class Parser {
        private final String text;
        private int pos;

        Parser(String text) {
            this.text = text;
        }

        boolean atEnd() {
            return pos >= text.length();
        }

        void expect(String token) {
            if (!text.startsWith(token, pos)) {
                throw new IllegalStateException("Corrupt data: expected '" + token
                        + "' at offset " + pos);
            }
            pos += token.length();
        }

        void expectChar(char c) {
            if (atEnd() || text.charAt(pos) != c) {
                throw new IllegalStateException("Corrupt data: expected '" + c
                        + "' at offset " + pos);
            }
            pos++;
        }

        long parseLong(char delimiter) {
            return Long.parseLong(parseToken(delimiter));
        }

        int parseInt(char delimiter) {
            return Integer.parseInt(parseToken(delimiter));
        }

        private String parseToken(char delimiter) {
            int end = text.indexOf(delimiter, pos);
            if (end < 0) {
                throw new IllegalStateException("Corrupt data: missing '" + delimiter
                        + "' after offset " + pos);
            }
            String token = text.substring(pos, end);
            pos = end + 1;
            return token;
        }

        Object parseValue() {
            if (atEnd()) {
                throw new IllegalStateException("Corrupt data: unexpected end of input");
            }
            char type = text.charAt(pos);
            pos++;
            switch (type) {
                case 'S': {
                    int length = parseInt(':');
                    if (length < 0 || pos + length > text.length()) {
                        throw new IllegalStateException(
                                "Corrupt data: bad string length at offset " + pos);
                    }
                    String s = text.substring(pos, pos + length);
                    pos += length;
                    return s;
                }
                case 'L':
                    return Long.valueOf(parseLong(';'));
                case 'B': {
                    if (atEnd()) {
                        throw new IllegalStateException(
                                "Corrupt data: truncated boolean at offset " + pos);
                    }
                    char flag = text.charAt(pos);
                    pos++;
                    if (flag == '1') {
                        return Boolean.TRUE;
                    }
                    if (flag == '0') {
                        return Boolean.FALSE;
                    }
                    throw new IllegalStateException(
                            "Corrupt data: bad boolean flag at offset " + (pos - 1));
                }
                case 'D':
                    return Double.valueOf(parseToken(';'));
                case 'A': {
                    int count = parseInt('{');
                    List<Object> list = new ArrayList<Object>(count);
                    for (int i = 0; i < count; i++) {
                        list.add(parseValue());
                    }
                    expectChar('}');
                    return list;
                }
                case 'M': {
                    int count = parseInt('{');
                    Map<String, Object> map = new LinkedHashMap<String, Object>();
                    for (int i = 0; i < count; i++) {
                        Object key = parseValue();
                        if (!(key instanceof String)) {
                            throw new IllegalStateException(
                                    "Corrupt data: map key is not a string at offset " + pos);
                        }
                        map.put((String) key, parseValue());
                    }
                    expectChar('}');
                    return map;
                }
                default:
                    throw new IllegalStateException("Corrupt data: unknown type tag '" + type
                            + "' at offset " + (pos - 1));
            }
        }
    }
}
