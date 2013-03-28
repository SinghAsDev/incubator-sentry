/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.access.tests.e2e;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
public class Util {
	private final static String dataFileDir = "src/test/resources";

	public static void addPolicy(String line, String cat) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(new Path(dataFileDir, "test-authz-provider.ini").toString()));

			// reading contents to a list
			// insert line into corresponding cat
			ArrayList<String> list = new ArrayList<String>();
			String s;
			while ((s = br.readLine()) != null) {
				list.add(s);
				if (s.equals("[" + cat + "]")) {
					list.add(line);
				}
			}
			br.close();

			// dump content to new file
			BufferedWriter bw = new BufferedWriter(new FileWriter(new Path(dataFileDir, "test-authz-provider.ini").toString()));
			for (int i = 0; i < list.size(); i++) {
				bw.write(list.get(i) + "\r\n");
			}
			bw.close();
		} catch (IOException e) {
            //TO-DO:nothing
		}
	}

	public static void removePolicy(String line) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(new Path(dataFileDir, "test-authz-provider.ini").toString()));

			// reading contents to a list
			// insert line into corresponding cat
			ArrayList<String> list = new ArrayList<String>();
			String s;
			while ((s = br.readLine()) != null) {
				if (!s.equals(line)) {
					list.add(s);
				}
			}
			br.close();

			// dump content to new file
			BufferedWriter bw = new BufferedWriter(new FileWriter(new Path(dataFileDir, "test-authz-provider.ini").toString()));
			for (int i = 0; i < list.size(); i++) {
				bw.write(list.get(i) + "\r\n");
			}
			bw.close();
		} catch (IOException e) {
            //TO-DO:nothing
		}
	}
}