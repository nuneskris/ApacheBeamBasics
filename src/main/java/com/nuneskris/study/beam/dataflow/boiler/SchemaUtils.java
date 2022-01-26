package com.nuneskris.study.beam.dataflow.boiler;


import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;

/** Schema utilities for KafkaToBigQuery pipeline. */
public class SchemaUtils {

    /* Logger for class. */
    private static final Logger LOG = LoggerFactory.getLogger(SchemaUtils.class);


    public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"CricketScore\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":\"inning\",\"type\":\"int\"},{\"name\":\"over\",\"type\":\"int\"},{\"name\":\"ball\",\"type\":\"int\"},{\"name\":\"batsman\",\"type\":\"string\"},{\"name\":\"non_striker\",\"type\":\"string\"},{\"name\":\"bowler\",\"type\":\"string\"},{\"name\":\"batsman_runs\",\"type\":\"int\"},{\"name\":\"extra_runs\",\"type\":\"int\"},{\"name\":\"total_runs\",\"type\":\"int\"},{\"name\":\"non_boundary\",\"type\":\"int\"},{\"name\":\"is_wicket\",\"type\":\"int\"},{\"name\":\"dismissal_kind\",\"type\":\"string\"},{\"name\":\"player_dismissed\",\"type\":\"string\"},{\"name\":\"fielder\",\"type\":\"string\"},{\"name\":\"extras_type\",\"type\":\"string\"},{\"name\":\"batting_team\",\"type\":\"string\"},{\"name\":\"bowling_team\",\"type\":\"string\"}]}");


    public static final String DEADLETTER_SCHEMA =
            "{\n"
                    + "  \"fields\": [\n"
                    + "    {\n"
                    + "      \"name\": \"timestamp\",\n"
                    + "      \"type\": \"TIMESTAMP\",\n"
                    + "      \"mode\": \"REQUIRED\"\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"name\": \"payloadString\",\n"
                    + "      \"type\": \"STRING\",\n"
                    + "      \"mode\": \"REQUIRED\"\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"name\": \"payloadBytes\",\n"
                    + "      \"type\": \"BYTES\",\n"
                    + "      \"mode\": \"REQUIRED\"\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"name\": \"attributes\",\n"
                    + "      \"type\": \"RECORD\",\n"
                    + "      \"mode\": \"REPEATED\",\n"
                    + "      \"fields\": [\n"
                    + "        {\n"
                    + "          \"name\": \"key\",\n"
                    + "          \"type\": \"STRING\",\n"
                    + "          \"mode\": \"NULLABLE\"\n"
                    + "        },\n"
                    + "        {\n"
                    + "          \"name\": \"value\",\n"
                    + "          \"type\": \"STRING\",\n"
                    + "          \"mode\": \"NULLABLE\"\n"
                    + "        }\n"
                    + "      ]\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"name\": \"errorMessage\",\n"
                    + "      \"type\": \"STRING\",\n"
                    + "      \"mode\": \"NULLABLE\"\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"name\": \"stacktrace\",\n"
                    + "      \"type\": \"STRING\",\n"
                    + "      \"mode\": \"NULLABLE\"\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}";

    /**
     * The {@link SchemaUtils#getGcsFileAsString(String)} reads a file from GCS and returns it as a
     * string.
     *
     * @param filePath path to file in GCS
     * @return contents of the file as a string
     * @throws IOException thrown if not able to read file
     */
    public static String getGcsFileAsString(String BUCKET_URL, String OBJECT_NAME) {
        Storage storage = StorageOptions.getDefaultInstance().getService();
        Blob blob = storage.get(BlobId.of(BUCKET_URL, OBJECT_NAME));
        String fileContent = new String(blob.getContent());
        return fileContent;
    }

    /**
     * The {@link SchemaUtils#getAvroSchema(String)} reads an Avro schema file from GCS, parses it and
     * returns a new {@link Schema} object.
     *
     * @param schemaLocation Path to schema (e.g. gs://mybucket/path/).
     * @return {@link Schema}
     */
    public static Schema getAvroSchema() {
        String schemaFile = getGcsFileAsString( "cricket-score-study",  "cricketscore.avsc");
        return new Schema.Parser().parse(schemaFile);
    }

    /**
     * @param avroschema avroschema in text format
     * @return {@link Schema}
     */
    public static Schema parseAvroSchema(@Nonnull String avroschema) {
        return new Schema.Parser().parse(avroschema);
    }
}