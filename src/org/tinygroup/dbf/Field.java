/**
 * Copyright (c) 2012-2016, www.tinygroup.org (luo_guo@icloud.com).
 * <p>
 * Licensed under the GPL, Version 3.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.gnu.org/licenses/gpl.html
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tinygroup.dbf;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by luoguo on 2014/4/25.
 */
public class Field {
    private String name;
    private int length;
    private int decimal;
    private char type;
    private byte flag;
    private int displacement;
    private ByteBuffer buffer;
    private String stringValue;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public int getDecimal() {
        return decimal;
    }

    public void setDecimal(int decimal) {
        this.decimal = decimal;
    }

    public char getType() {
        return type;
    }

    public void setType(char type) {
        this.type = type;
    }

    public byte getFlag() {
        return flag;
    }

    public void setFlag(byte flag) {
        this.flag = flag;
    }

    public int getDisplacement() {
        return displacement;
    }

    public void setDisplacement(int displacement) {
        this.displacement = displacement;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public void setBuffer(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    public byte[] getBytesValue() {
        return buffer.array();
    }

    public String getStringValue() throws UnsupportedEncodingException {
        return stringValue;
    }

    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    public Integer getIntValue() throws UnsupportedEncodingException {
        if (("-").equals(getStringValue())) {
            return null;
        }
        return Integer.parseInt(getStringValue());
    }

    public Double getDoubleValue() throws UnsupportedEncodingException {
        if (("-").equals(getStringValue())) {
            return null;
        }
        return Double.parseDouble(getStringValue());
    }

    public Float getFloatValue() throws UnsupportedEncodingException {
        if (("-").endsWith(getStringValue())) {
            return null;
        }
        return Float.parseFloat(getStringValue());
    }

    public Boolean getBooleanValue() throws UnsupportedEncodingException {
        String value = getStringValue();
        if (value == null || value.length() == 0) {
            return null;
        } else {
            return Util.getBooleanValue(value);
        }
    }

    public Date getDateValue() throws ParseException, UnsupportedEncodingException {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        return format.parse(getStringValue());
    }
}
