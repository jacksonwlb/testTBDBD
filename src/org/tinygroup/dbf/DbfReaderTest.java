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

import java.io.IOException;

/**
 * Created by luoguo on 2014/4/25.
 */
public class DbfReaderTest {
    static String[] files = {"OFPROFILE20140401"};

    public static void main(String[] args) throws IOException, IllegalAccessException, InstantiationException {
        
    	String destDBF = "E:\\PROJECT\\OKAO\\data\\录取库\\11_北京\\本科一批（理工）_39\\T_TDD.dbf";
    	
    	printFile(destDBF);
    }

    public static void printFile(String fileName) throws IOException, InstantiationException, IllegalAccessException {
        Reader dbfReader = DbfReader.parse(fileName);
        for (Field field : dbfReader.getFields()) {
            System.out.printf("name:%s %s(%d,%d)\n", field.getName(), field.getType(), field.getLength(), field.getDecimal());
        }
        System.out.println();
        for (int i = 0; i < dbfReader.getHeader().getRecordCount(); i++) {
            dbfReader.next();
            for (Field field : dbfReader.getFields()) {
                if (field.getName().startsWith("ISSTAT")) {
                    System.out.print("");
                }
                System.out.printf("%" + field.getLength() + "s", field.getStringValue().trim());
            }
            System.out.println();
        }
        dbfReader.close();

    }
}
