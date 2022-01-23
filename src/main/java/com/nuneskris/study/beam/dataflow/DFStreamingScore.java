package com.nuneskris.study.beam.dataflow;

/*
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


import com.nuneskris.study.beam.BeamScore;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.IOException;

/**
 * A streaming Beam Example using BigQuery output.
 *
 * <p>This pipeline example reads lines of the input text file, splits each line into individual
 * words, capitalizes those words, and writes the output to a BigQuery table.
 *
 * <p>The example is configured to use the default BigQuery table from the example common package
 * (there are no defaults for a general Beam pipeline). You can override them by using the {@literal
 * --bigQueryDataset}, and {@literal --bigQueryTable} options. If the BigQuery table do not exist,
 * the example will try to create them.
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class DFStreamingScore {

    public interface StreamingScoreExtractOptions
            extends StudyOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://cricket-score-study/IPLBall-by-Ball 2008-2020.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to read from")
        @Default.String("gs://cricket-score-study/output")
        String getOutputFile();
        void setOutputFile(String value);
    }

    /**
     * Sets up and starts streaming pipeline.
     *
     * @throws IOException if there is a problem setting up resources
     */
    public static void main(String[] args) throws IOException {
        System.out.println("processGCPlViaPardo");
        StreamingScoreExtractOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(StreamingScoreExtractOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new BeamScore.ExtractScore()))
                .apply(ParDo.of(new BeamScore.FilterWickets()))
                .apply(ParDo.of(new BeamScore.ConvertToKV()))
                .apply(GroupByKey.<String, Integer>create())
                .apply(ParDo.of(new BeamScore.SumUpValuesByKey()))
                .apply("WriteCounts.csv",TextIO.write().to(options.getOutputFile()).withoutSharding())
        ;
        pipeline.run();

    }
}