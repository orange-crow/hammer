package com.example.featurecomputer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import feature.Feature; // 由proto生成的Feature类
import feature.Source;  // 由proto生成的Source类
// import feature.Entity; // 根据需要引入

public class FeatureComputer {

    public static void main(String[] args) {
        // 初始化SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("FeatureComputer")
                .master("local[*]") // 测试时使用，本地模式；生产环境建议移除此设置
                .getOrCreate();

        // 模拟常驻服务，不断接收Feature消息（这里只处理单个示例）
        Feature feature = receiveFeature();

        // 根据feature.source中的信息加载数据源（假设Source中包含数据路径和格式）
        Source source = feature.getSource();
        String dataPath = source.getDataPath();   // 例如："/data/source_data"
        String dataFormat = source.getFormat();     // 例如："parquet"
        Dataset<Row> sourceDF = spark.read().format(dataFormat).load(dataPath);

        // 将数据注册为临时视图，供Spark SQL使用
        sourceDF.createOrReplaceTempView("source_table");

        // 使用Feature.transform字段中的转换逻辑（Spark SQL查询）
        String sqlQuery = feature.getTransform();
        Dataset<Row> transformedDF = spark.sql(sqlQuery);

        // 根据feature.sink中的配置信息将结果写入指定的输出目标
        // 假设sink map中包含 "format" 和 "path" 两个键
        String sinkFormat = feature.getSinkOrDefault("format", "parquet");
        String sinkPath = feature.getSinkOrDefault("path", "/tmp/feature_output");
        transformedDF.write().format(sinkFormat).mode("overwrite").save(sinkPath);

        // 可以在这里更新Feature状态、日志记录或执行其他后续操作
        System.out.println("Feature computation completed for: " + feature.getName());

        spark.stop();
    }