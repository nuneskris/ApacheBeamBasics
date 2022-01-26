package com.nuneskris.study.beam.dataflow.boiler;


import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.apache.avro.Schema;

public class AvroSchemaUtil {
    public static void main(String args[]){
        Schema schema = AvroSchemaUtil.getSchema(args[0], args[1]);
        for( Schema.Field field: schema.getFields()){
            System.out.println(field.name());
        }
    }
    public static Schema getSchema(String bucketName, String blobName){
        Storage storage = StorageOptions.getDefaultInstance().getService();
        BlobId blobId = BlobId.of(bucketName, blobName);
        Blob blob = storage.get(blobId);
        String value = new String(blob.getContent());
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(value);
    }
}

