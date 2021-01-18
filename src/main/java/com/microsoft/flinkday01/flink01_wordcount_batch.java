package com.microsoft.flinkday01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;

/**
 * @author Jenny.D
 * @create 2021-01-18 11:27
 */
public class flink01_wordcount_batch {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> input = env.readTextFile("Input");
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOneDS = input.flatMap(new MyFlatMapFunc());
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOneDS.groupBy(0);


        //wordToOneDS.print();

       //groupBy.print();

    }

    public static class MyFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String,Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for(String word:words){
                out.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }
}


