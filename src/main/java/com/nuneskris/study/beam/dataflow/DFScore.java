package com.nuneskris.study.beam.dataflow;

import com.nuneskris.study.beam.BeamScore;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import java.io.IOException;

public class DFScore {

    public interface ScoreExtractOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("gs://cricket-score-study/IPLBall-by-Ball 2008-2020.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to read from")
        @Default.String("gs://cricket-score-study/output.csv")
        String getOutputFile();
        void setOutputFile(String value);
    }

    public static void main(String[] args) throws IOException {
        ScoreExtractOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(ScoreExtractOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new BeamScore.ExtractScore()))
                .apply(ParDo.of(new BeamScore.FilterWickets()))
                .apply(ParDo.of(new BeamScore.ConvertToKV()))
                .apply(GroupByKey.<String, Integer>create())
                .apply(ParDo.of(new BeamScore.SumUpValuesByKey()))
                .apply("WriteCounts.csv", TextIO.write().to(options.getOutputFile()).withoutSharding())
        ;
        pipeline.run();
    }
}