package com.nuneskris.study.beam.dataflow.streaming;

import com.nuneskris.study.beam.dataflow.boiler.SchemaUtils;
import org.apache.avro.Schema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.Row;

import java.io.IOException;
import java.util.List;

public class StreamingDataFlow {
    /**
     * Provides custom {@link org.apache.beam.sdk.options.PipelineOptions} required to execute the
     * {@linkPubsubAvroToBigQuery} pipeline.
     */
    public interface PubsubAvroToBigQueryOptions
            extends PubsubCommonOptions.ReadSubscriptionOptions {

        @Description("GCS path to Avro schema file.")
        @Validation.Required
        @Default.String("gs://cricket-score-study")
        String getSchemaBucketPath();

        void setSchemaBucketPath(String schemaBucketPath);

        @Description("GCS path to Avro schema file.")
        @Validation.Required
        @Default.String("gs://cricket-score-study")
        String getSchemaFilePath();

        void setSchemaFilePath(String schemaFilePath);
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
                .apply("WriteCounts.csv", TextIO.write().to("gs://cricket-score-study/outputavro.csv").withoutSharding());
        return null;
    }
}
