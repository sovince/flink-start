package com.sovince.batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

import java.util.ArrayList;

/**
 * Created by vince
 * Email: so_vince@outlook.com
 * Data: 2019/6/11
 * Time: 23:18
 * Description:
 */
public class JavaTransTest {
    private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    public static void mapFunction() throws Exception {
        ArrayList<Integer> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);
        data
                .map(new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer integer) {
                        return integer * 10;
                    }
                })
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer integer) throws Exception {
                        return integer>50;
                    }
                })
                .print()
        ;
    }

    public static void main(String[] args) throws Exception {
        mapFunction();
    }
}
