package com.nuneskris.study.beam.dataflow.streaming;

import com.nuneskris.study.beam.dataflow.boiler.SchemaUtils;
import com.nuneskris.study.beam.dataflow.boiler.WindowedFilenamePolicy;
import com.nuneskris.study.beam.dataflow.boiler.WindowedFilenamePolicyOptions;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.List;

public class StreamingDataFlow {
    /**
     * Provides custom {@link org.apache.beam.sdk.options.PipelineOptions} required to execute the
     * {@linkPubsubAvroToBigQuery} pipeline.
     */
    public interface PubsubAvroToBigQueryOptions
            extends PipelineOptions, StreamingOptions, WindowedFilenamePolicyOptions {
        @Description(
                "The Cloud Pub/Sub subscription to consume from. "
                        + "The name should be in the format of "
                        + "projects/<project-id>/subscriptions/<subscription-name>.")

        ValueProvider<String> getInputSubscription();

        void setInputSubscription(ValueProvider<String> value);

        @Description("The Cloud Pub/Sub topic to read from.")
        ValueProvider<String> getInputTopic();

        void setInputTopic(ValueProvider<String> value);

        @Description(
                "This determines whether the template reads from " + "a pub/sub subscription or a topic")
        @Default.Boolean(true)
        Boolean getUseSubscription();

        void setUseSubscription(Boolean value);

        @Description("The directory to output files to. Must end with a slash.")
        @Validation.Required
        ValueProvider<String> getOutputDirectory();

        void setOutputDirectory(ValueProvider<String> value);

        @Description("The filename prefix of the files to write to.")
        @Default.String("Output")
        ValueProvider<String> getOutputFilenamePrefix();

        void setOutputFilenamePrefix(ValueProvider<String> value);

        @Description("The suffix of the files to write.")
        @Default.String("avro")
        ValueProvider<String> getOutputFilenameSuffix();

        void setOutputFilenameSuffix(ValueProvider<String> value);

        @Description("The Avro Write Temporary Directory. Must end with /")
        @Validation.Required
        ValueProvider<String> getAvroTempDirectory();

        void setAvroTempDirectory(ValueProvider<String> value);
    }


    public static void main(String[] args) throws IOException {
        PubsubAvroToBigQueryOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(PubsubAvroToBigQueryOptions.class);

        run(options);

    }
    public static class ConvertRowToString extends DoFn<org.apache.beam.sdk.values.Row, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            Row row = c.element();
            List<Schema.Field> fields = schema.getFields();
            String output = "";
            for(Schema.Field field: fields){
                output = output + row.getString(field.name()) + ",";
            }
            output = output.replaceAll(".$", "");
            c.output(output);
        }
    }

    static Schema schema;
    public static PipelineResult run(PubsubAvroToBigQueryOptions options) {
        Pipeline pipeline = Pipeline.create(options);
        schema = SchemaUtils.SCHEMA$;
        pipeline
                .apply(
                        "Read Avro records",
                        PubsubIO.readAvroGenericRecords(schema)
                                .fromSubscription(options.getInputSubscription()))
                .apply(Convert.toRows())
                .apply(ParDo.of(new ConvertRowToString()))
                .apply(
                        Window.<String>into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply("WriteCounts.csv", TextIO.write().to(
                                WindowedFilenamePolicy.writeWindowedFiles()
                                        .withOutputDirectory(options.getOutputDirectory())
                                        .withOutputFilenamePrefix(options.getOutputFilenamePrefix())
                                        .withShardTemplate(options.getOutputShardTemplate())
                                        .withSuffix(options.getOutputFilenameSuffix())
                                        .withYearPattern(options.getYearPattern())
                                        .withMonthPattern(options.getMonthPattern())
                                        .withDayPattern(options.getDayPattern())
                                        .withHourPattern(options.getHourPattern())
                                        .withMinutePattern(options.getMinutePattern()))
                        .withTempDirectory(
                                ValueProvider.NestedValueProvider.of(
                                        options.getAvroTempDirectory(),
                                        (SerializableFunction<String, ResourceId>)
                                                input -> FileBasedSink.convertToFileResourceIfPossible(input)))
                        /*.withTempDirectory(FileSystems.matchNewResource(
                        options.getAvroTempDirectory(),
                        Boolean.TRUE))
                        */
                        .withWindowedWrites()
                        .withNumShards(options.getNumShards()));
        return null;
    }
}
