package com.nuneskris.study.beam;

import org.apache.avro.generic.GenericRecord;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.SerializationUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/**
 * batch pipeline.
 * Input file(s)
 * This project can be run locally.
 * should be a text log in the following format:
 * "id","inning","over","ball","batsman","non_striker","bowler","batsman_runs","extra_runs","total_runs","non_boundary","is_wicket","dismissal_kind","player_dismissed","fielder","extras_type","batting_team","bowling_team"
 */
public class HelloBeam {


    private static PCollection<String> getLocalData(Pipeline pipeline) {
        return pipeline.apply(TextIO.read().from("gs://cricket-score-study/IPLBall-by-Ball 2008-2020.csv"));
    }
    // ******** Function: MapElements *************************
    private static PCollection<String[]> parseMapElementsSplitString(PCollection<String> input) {
        return
                input.apply(
                        "parse",
                        MapElements.via(
                                new SimpleFunction<String, String[]>() {
                                    @Override
                                    public String[] apply(String input) {
                                        String[] split = input.split(",");
                                        return split;
                                    }
                                }));
    }


    // ******** Function: Filter *************************
    private static PCollection<String[]> filterWickets(PCollection<String[]> input) {
        return input.apply("filterWickets",
                (Filter.by(new SerializableFunction<String[], Boolean>() {
                    @Override
                    public Boolean apply(String[] input) {
                        return input[11].equalsIgnoreCase("1");
                    }
                })));
    }



    // ******** Class: KV *************************
    private static PCollection<KV<String, Integer>> convertToKV(PCollection<String[]> filterWickets){
            return filterWickets.apply(
                    "convertToKV",
                    MapElements.via(
                    new SimpleFunction<String[], KV<String, Integer>>()
                {
                    @Override
                    public KV<String, Integer> apply (String[]input){
                        String[] split = {input[11]};
                        String key = input[4] + "," + input[12];
                    return KV.of(key, new Integer(1));
                }
                }));
    }



    // ******** Function: GroupByKey *************************
   private static PCollection<KV<String, Iterable<Integer>>> groupByKeysOfKV(PCollection<KV<String, Integer>>  convertToKV) {
       return convertToKV.apply(
                GroupByKey.<String, Integer>create()
        );
    }

    // ******** Function: ParDo *************************
    //we can have a pardo function inside if we not want to reuse it.
    private static PCollection<String> sumUpValuesByKey(PCollection<KV<String, Iterable<Integer>>> kvpCollection){
        return    kvpCollection.apply(
                "SumUpValuesByKey",
                ParDo.of(
                        new DoFn<KV<String, Iterable<Integer>>, String>() {

                            @ProcessElement
                            public void processElement(ProcessContext context) {
                                Integer totalWickets = 0;
                                String playerAndWicketType = context.element().getKey();
                                Iterable<Integer> wickets = context.element().getValue();
                                for (Integer amount : wickets) {
                                    totalWickets += amount;
                                }
                                context.output(playerAndWicketType + "," + totalWickets);
                            }
                        }));

    }



    private static void writeLocally(PCollection<String> sumUpValuesByKey){
        sumUpValuesByKey.apply(TextIO.write().to("/Users/krisnunes/Desktop/Study/archive/IPLOuts.csv").withoutSharding());
    }

    private static void processLocalViaPardo(){
        System.out.println("processLocalViaPardo");
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(TextIO.read().from("/Users/krisnunes/Desktop/Study/archive/IPLBall-by-Ball 2008-2020.csv"))
                .apply(ParDo.of(new BeamScore.ExtractScore()))
                .apply(ParDo.of(new BeamScore.FilterWickets()))
                .apply(ParDo.of(new BeamScore.ConvertToKV()))
                .apply(GroupByKey.<String, Integer>create())
                .apply(ParDo.of(new BeamScore.SumUpValuesByKey()))
                .apply(TextIO.write().to("/Users/krisnunes/Desktop/Study/archive/IPLOutsParDo.csv").withoutSharding())
                ;
        pipeline.run();
    }
    public interface Options
            extends PipelineOptions{

    }
    private static void joinUseCase(){
        GcpOptions options = PipelineOptionsFactory.as(GcpOptions.class);
        options.setTempLocation("gs://cricket-score-study/temp");
        Pipeline pipeline = Pipeline.create(options);
        PCollection<String> localData = getLocalData(pipeline);
        PCollection<PojoScore> extractScoreAsObject =  localData .apply(ParDo.of(new BeamScore.ExtractScoreAsObject()));
        PCollection<KV<String, Integer>> kv = extractScoreAsObject.apply(ParDo.of(new BeamScore.ConvertToKVForBatsman_runs()));

        PCollection<KV<String, Iterable<Integer>>> kvIterableScores = kv.apply(GroupByKey.<String, Integer>create());

        PCollection<KV<String, PojoScore>> sumUpBatsmanRunsLeft = kvIterableScores.apply(ParDo.of(new BeamScore.SumUpBatsmanRuns()));

        PCollection<String> newOne =   pipeline.apply(TextIO.read().from("gs://cricket-score-study/IPL Matches 2008-2020.csv"));
        PCollection<KV<String, PojoMatch>>  matchesRight =newOne.apply(ParDo.of(new BeamScore.ExtractMatchAsObject()));

        final TupleTag<PojoScore> t1 = new TupleTag<>();
        final TupleTag<PojoMatch> t2 = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> coGbkResultCollection =
                KeyedPCollectionTuple.of(t1, sumUpBatsmanRunsLeft)
                        .and(t2, matchesRight)
                        .apply(CoGroupByKey.<String>create());

        PCollection<PojoScore> finalResultCollection =
                coGbkResultCollection.apply(ParDo.of(
                        new DoFn<KV<String, CoGbkResult>, PojoScore>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<String, CoGbkResult> e = c.element();
                                Iterable<PojoScore> scores = e.getValue().getAll(t1);
                                PojoMatch match = e.getValue().getOnly(t2);
                                for(PojoScore ss : scores){
                                    PojoScore scoreNew = SerializationUtils.clone(ss);
                                    scoreNew.setMatch(match);
                                    c.output(scoreNew);
                                }
                            }
                        }));
        PCollection<String> simplyPrintVals = finalResultCollection.apply(ParDo.of( new DoFn<PojoScore, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                PojoScore s = c.element();
                c.output(s.getId() +","+ s.getBatsman()  +","+s.getTotal_runs()  +","+s.getMatch().getPlayer_of_match());
            }
        }
        ));

       // PCollection<AvroScore>  avroScoresCol = finalResultCollection.apply(ParDo.of(new BeamScore.ConvertToAvroSpecific()));
        //avroScoresCol.apply(AvroIO.write(AvroScore.class).to("/Users/krisnunes/Study/archive/file.avro"));
       // simplyPrintVals.apply(TextIO.write().to("/Users/krisnunes/Study/archive/archive/test.csv").withoutSharding());



        PCollection<GenericRecord>  avroScoresGenRec = finalResultCollection.apply(ParDo.of(new BeamScore.ConvertToAvroGeneric()));
        avroScoresGenRec.setCoder(AvroUtils.schemaCoder(AvroScore.SCHEMA$));
        avroScoresGenRec.apply(
                "Write to BigQuery",
                BigQueryIO.<GenericRecord>write()
                        .to("java-maven-dataflow:avrotest.avrotab")
                        .useBeamSchema()

                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .optimizedWrites());

        pipeline.run();
    }



    private static void processLocal(){
        Pipeline pipeline = Pipeline.create();

        writeLocally(
           sumUpValuesByKey(
               groupByKeysOfKV(
                   convertToKV(
                       filterWickets(
                           parseMapElementsSplitString(
                               getLocalData(pipeline)))))));



        pipeline.run();
    }
    public static final void main(String args[]) throws Exception {
       // processLocal();
        // processLocalViaPardo();
        joinUseCase();
    }
}