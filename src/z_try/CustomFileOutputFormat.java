/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package z_try;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by TAN on 6/23/2015.
 */

public class CustomFileOutputFormat extends FileOutputFormat<LongWritable, byte[]> {

    public RecordWriter<LongWritable, byte[]> getRecordWriter(TaskAttemptContext context)
            throws IOException {

        Configuration conf = context.getConfiguration();

        Path path = super.getDefaultWorkFile(context, "");

        FileSystem fs = path.getFileSystem(conf);
        final FSDataOutputStream fileOutputStream = fs.create(path, true); // boolean overwrite = true

        return new RecordWriter<LongWritable, byte[]>() {

            public void write(LongWritable key, byte[] value)
                    throws IOException {
                fileOutputStream.write(value);
            }

            public void close(TaskAttemptContext context) throws IOException {
                fileOutputStream.close();
            }
        };
    }
}