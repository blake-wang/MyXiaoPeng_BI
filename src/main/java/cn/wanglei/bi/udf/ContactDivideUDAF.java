package cn.wanglei.bi.udf;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

import static org.apache.spark.sql.types.DataTypes.StringType;

/**
 * Created by bigdata on 17-7-28.
 */
public class ContactDivideUDAF extends UserDefinedAggregateFunction {
    public StructType inputSchema() {
        return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("inputSal", StringType, true)));
    }

    public StructType bufferSchema() {
        return DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("s1", StringType, true)));
    }

    public DataType dataType() {
        return StringType;
    }

    public boolean deterministic() {
        return true;
    }

    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, "");
    }

    public void update(MutableAggregationBuffer buffer, Row input) {
        //获取传入的值
        String s1 = buffer.getString(0);
        if (s1 == null) {
            s1 = "";
        }
        //获取以前的数据
        String inputSql = input.getString(0);
        if (inputSql == null) {
            inputSql = "";
        }
        //更新缓存的数据
        buffer.update(0, s1 + inputSql);
    }

    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        //分别获取缓存区的数据
        String s11 = buffer1.getString(0);
        if (s11 == null) {
            s11 = "";
        }
        String s12 = buffer2.getString(0);
        if (s12 == null) {
            s12 = "";
        }
        //更新缓存
        buffer1.update(0, s11 + "|" + s12);
    }

    public Object evaluate(Row buffer) {
        return buffer.getString(0);
    }
}
