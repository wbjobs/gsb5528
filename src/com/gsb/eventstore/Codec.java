package com.gsb.eventstore;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Stable, self-delimiting text encoding for event payloads.
 *
 * Grammar (all tags are single ASCII bytes, numbers are ASCII decimal):
 *   value := 'S' &lt;byteLen&gt; ':' &lt;utf8 bytes&gt;      String (length-prefixed, binary safe)
 *          | 'L' &lt;digits&gt; ';'                          Long
 *          | 'B' ('t' | 'f') ';'                            Boolean
 *          | 'D' &lt;Double.toString&gt; ';'                  Double
 *          | 'A' &lt;count&gt; ':' value*                     List
 *          | 'M' &lt;count&gt; ':' (key value)*               Map (keys are S-encoded Strings)
 *
 * Because every value is either length-prefixed or count-prefixed, a stream of
 * values can be decoded sequentially with no escaping and no ambiguity.
 */
final class Codec {

    private static final Charset UTF8 = Charset.forName("UTF-8");

    private Codec() {
    }

    static byte[] encode(Map<String, Object> payload) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writeValue(out, payload);
        return out.toByteArray();
    }

    static Decoder decoder(byte[] data) {
        return new Decoder(data);
    }

    private static void writeValue(ByteArrayOutputStream out, Object value) {
        if (value instanceof String) {
            byte[] bytes = ((String) value).getBytes(UTF8);
            out.write('S');
            writeAscii(out, Integer.toString(bytes.length));
            out.write(':');
            out.write(bytes, 0, bytes.length);
        } else if (value instanceof Long) {
            out.write('L');
            writeAscii(out, value.toString());
            out.write(';');
        } else if (value instanceof Boolean) {
            out.write('B');
            out.write(((Boolean) value).booleanValue() ? 't' : 'f');
            out.write(';');
        } else if (value instanceof Double) {
            out.write('D');
            writeAscii(out, Double.toString(((Double) value).doubleValue()));
            out.write(';');
        } else if (value instanceof List) {
            List<?> list = (List<?>) value;
            out.write('A');
            writeAscii(out, Integer.toString(list.size()));
            out.write(':');
            for (Object element : list) {
                writeValue(out, element);
            }
        } else if (value instanceof Map) {
            Map<?, ?> map = (Map<?, ?>) value;
            out.write('M');
            writeAscii(out, Integer.toString(map.size()));
            out.write(':');
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                writeValue(out, entry.getKey());
                writeValue(out, entry.getValue());
            }
        } else {
            throw new IllegalArgumentException(
                    "Unsupported value type: " + (value == null ? "null" : value.getClass().getName()));
        }
    }

    private static void writeAscii(ByteArrayOutputStream out, String text) {
        for (int i = 0; i < text.length(); i++) {
            out.write((byte) text.charAt(i));
        }
    }

    static final class Decoder {
        private final byte[] data;
        private int pos;

        Decoder(byte[] data) {
            this.data = data;
        }

        boolean hasMore() {
            return pos < data.length;
        }

        Object readValue() {
            if (pos >= data.length) {
                throw new IllegalStateException("Corrupt data: unexpected end of input");
            }
            char tag = (char) (data[pos++] & 0xFF);
            switch (tag) {
                case 'S': {
                    int len = readInt(':');
                    if (len < 0 || pos + len > data.length) {
                        throw new IllegalStateException("Corrupt data: bad string length " + len);
                    }
                    String s = new String(data, pos, len, UTF8);
                    pos += len;
                    return s;
                }
                case 'L':
                    return Long.valueOf(readToken(';'));
                case 'B': {
                    if (pos >= data.length) {
                        throw new IllegalStateException("Corrupt data: truncated boolean");
                    }
                    char c = (char) (data[pos++] & 0xFF);
                    expect(';');
                    if (c == 't') {
                        return Boolean.TRUE;
                    }
                    if (c == 'f') {
                        return Boolean.FALSE;
                    }
                    throw new IllegalStateException("Corrupt data: bad boolean '" + c + "'");
                }
                case 'D':
                    return Double.valueOf(readToken(';'));
                case 'A': {
                    int count = readInt(':');
                    List<Object> list = new ArrayList<Object>(count);
                    for (int i = 0; i < count; i++) {
                        list.add(readValue());
                    }
                    return list;
                }
                case 'M': {
                    int count = readInt(':');
                    Map<String, Object> map = new LinkedHashMap<String, Object>();
                    for (int i = 0; i < count; i++) {
                        Object key = readValue();
                        if (!(key instanceof String)) {
                            throw new IllegalStateException("Corrupt data: map key is not a string");
                        }
                        map.put((String) key, readValue());
                    }
                    return map;
                }
                default:
                    throw new IllegalStateException("Corrupt data: unknown tag '" + tag + "'");
            }
        }

        void expect(char c) {
            if (pos >= data.length || (char) (data[pos] & 0xFF) != c) {
                throw new IllegalStateException("Corrupt data: expected '" + c + "' at offset " + pos);
            }
            pos++;
        }

        int readInt(char terminator) {
            String token = readToken(terminator);
            try {
                return Integer.parseInt(token);
            } catch (NumberFormatException e) {
                throw new IllegalStateException("Corrupt data: bad number '" + token + "'", e);
            }
        }

        private String readToken(char terminator) {
            int start = pos;
            while (pos < data.length && data[pos] != (byte) terminator) {
                pos++;
            }
            if (pos >= data.length) {
                throw new IllegalStateException("Corrupt data: missing '" + terminator + "'");
            }
            String token = new String(data, start, pos - start, UTF8);
            pos++;
            return token;
        }
    }
}
