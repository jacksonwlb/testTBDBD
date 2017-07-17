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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by luoguo on 2014/4/25.
 */
public abstract class DbfReader implements Reader {
	public static final int HEADER_END_CHAR = 13;
	private byte type;
	private String encode = "GBK";
	private FileChannel fileChannel;
	private Header header;
	private List<Field> fields;
	private boolean recordRemoved;
	private int position = 0;

	public static Reader parse(String dbfFile, String encode)
			throws IOException, IllegalAccessException, InstantiationException {
		return parse(new File(dbfFile), encode);
	}

	public static Reader parse(String dbfFile) throws IOException, IllegalAccessException, InstantiationException {
		return parse(new File(dbfFile), "GBK");
	}

	public static Reader parse(File dbfFile) throws IOException, IllegalAccessException, InstantiationException {
		return parse(dbfFile, "GBK");
	}

	public static Reader parse(File dbfFile, String encode)
			throws IOException, IllegalAccessException, InstantiationException {
		RandomAccessFile aFile = new RandomAccessFile(dbfFile, "r");
		FileChannel fileChannel = aFile.getChannel();
		ByteBuffer byteBuffer = ByteBuffer.allocate(1);
		fileChannel.read(byteBuffer);
		DbfReader reader = new FoxproDBaseReader();
		reader.setType(byteBuffer.array()[0]);
		reader.setFileChannel(fileChannel);
		reader.readHeader();
		reader.readFields();
		reader.skipHeaderTerminator();
		return reader;
	}

	@Override
	public int getRecordCount() {
		int ret = 0;
		try {
			
			this.moveBeforeFirst();
			
			while(this.hasNext())
			{
				this.next();
				++ret;
			}
			
			this.moveBeforeFirst();
			return ret;
			
		} catch (Exception e) {
			// TODO: handle exception
		}
		return -1;
	}
	
	public <T> void parseRecord(T t, List<Field> fields) {
		Class<? extends Object> thisClass = t.getClass();

		for (Field field : fields) {
			java.lang.reflect.Field reflectField = null;
			try {
				reflectField = thisClass.getDeclaredField(field.getName().toLowerCase());

				if (reflectField != null) {
					// reflectField.setAccessible(true);
					reflectField.set(t, field.getStringValue());
				}
			} catch (Exception e) {
				// TODO: handle exception
				e.printStackTrace();
			}

		}
	};
	
	public <T> void parseRecord(T t, Map<String,String> fieldMap) {
		Class<? extends Object> thisClass = t.getClass();

		
		for (java.lang.reflect.Field field : thisClass.getDeclaredFields()) {
			
			String fieldValue = fieldMap.get(field.getName());
			if(fieldValue !=null)
				try {
					field.set(t, fieldValue);
				} catch (IllegalArgumentException | IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	};

	public <T> T parseRecord(Class<T> c, List<Field> fields) {

		try {
			T t = c.newInstance();

			for (Field field : fields) {
				java.lang.reflect.Field reflectField = null;

				reflectField = c.getDeclaredField(field.getName().toLowerCase());
				if (reflectField != null) {
					reflectField.set(t, field.getStringValue());
				}
			}

			return t;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

		return null;
	};
	
	public <T> T parseRecord(Class<T> c, Map<String,String> fieldMap) {

		try {
			T t = c.newInstance();

			for (java.lang.reflect.Field field : c.getDeclaredFields()) {
				
				String fieldValue = fieldMap.get(field.getName());
				if(fieldValue !=null)
					try {
						field.set(t, fieldValue);
					} catch (IllegalArgumentException | IllegalAccessException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}

			return t;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

		return null;
	};

	public <T> T parseNextRecord(Class<T> c) {
		try {
			this.next();

			T t = parseRecord(c, getFieldMap());

			return t;
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

		return null;
	}

	public <T> List<T> parseRecordList(Class<T> c) {
		List<T> retList = new ArrayList<>();

		try {

			while (this.hasNext()) {
				T temp = this.parseNextRecord(c);
				retList.add(temp);
			}

		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}

		return retList;
	}

	public <T> Map<String, T> parseRecordMap(Class<T> c, String keyFieldName){
		Map<String, T> map = new HashMap<>();

		try {
			java.lang.reflect.Field keyField = c.getDeclaredField(keyFieldName);
			while (this.hasNext()) {
				T temp = this.parseNextRecord(c);

				map.put((String) keyField.get(temp), temp);

			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		

		return map;
	}

	public byte getType() {
		return type;
	}

	public void setType(byte type) {
		this.type = type;
	}

	public String getEncode() {
		return encode;
	}

	public Header getHeader() {
		return header;
	}

	public void setHeader(Header header) {
		this.header = header;
	}

	public List<Field> getFields() {
		return fields;
	}
	
	public Map<String, String> getFieldMap() throws UnsupportedEncodingException {
		Map<String, String> map = new HashMap<>();
		for (Field field : fields) {
			map.put(field.getName().toLowerCase(), field.getStringValue());
		}
		return map;
	}

	public void setFields(List<Field> fields) {
		this.fields = fields;
	}

	public boolean isRecordRemoved() {
		return recordRemoved;
	}

	public void setFileChannel(FileChannel fileChannel) {
		this.fileChannel = fileChannel;
	}

	protected abstract void readFields() throws IOException;

	public void moveBeforeFirst() throws IOException {
		position = 0;
		fileChannel.position(header.getHeaderLength());
	}

	/**
	 * @param position
	 *            从1开始
	 * @throws java.io.IOException
	 */
	public void absolute(int position) throws IOException {
		checkPosition(position);
		this.position = position;
		fileChannel.position(header.getHeaderLength() + (position - 1) * header.getRecordLength());
	}

	private void checkPosition(int position) throws IOException {
		if (position >= header.getRecordCount()) {
			throw new IOException("期望记录行数为" + (this.position + 1) + "，超过实际记录行数：" + header.getRecordCount() + "。");
		}
	}

	protected abstract Field readField() throws IOException;

	protected abstract void readHeader() throws IOException;

	private void skipHeaderTerminator() throws IOException {
		ByteBuffer byteBuffer = ByteBuffer.allocate(1);
		readByteBuffer(byteBuffer);
		if (byteBuffer.array()[0] != HEADER_END_CHAR) {
			throw new IOException("头结束符期望是13，但实际是：" + byteBuffer.array()[0]);
		}
	}

	public void close() throws IOException {
		fileChannel.close();
	}

	public void next() throws IOException {
		checkPosition(position);
		ByteBuffer byteBuffer = ByteBuffer.allocate(1);
		readByteBuffer(byteBuffer);
		this.recordRemoved = (byteBuffer.array()[0] == '*');
		for (Field field : fields) {
			if (field.getType() == 'M' || field.getType() == 'B' || field.getType() == 'G') {
				throw new IOException("Not Support type Memo");
			}
			read(field);
		}
		position++;
	}

	public boolean hasNext() {
		return position < header.getRecordCount();
	}

	private void read(Field field) throws IOException {
		ByteBuffer buffer = ByteBuffer.allocate(field.getLength());
		readByteBuffer(buffer);
		field.setStringValue(new String(buffer.array(), encode).trim());
		field.setBuffer(buffer);
	}

	protected void readByteBuffer(ByteBuffer byteBuffer) throws IOException {
		fileChannel.read(byteBuffer);
	}
}
