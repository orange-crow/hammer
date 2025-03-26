package com.hammer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FeatureComputer {
    public static void main(String[] args) {
        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Spark PostgreSQL Example")
                .master("local[*]")
                .getOrCreate();

        try {
            // PostgreSQL连接参数
            String jdbcUrl = "jdbc:postgresql://localhost:5432/mldata";
            String table = "test_format1";
            String user = "hammer_dev1";
            String password = "hammer123";

            // 使用Spark SQL读取PostgreSQL表
            Dataset<Row> jdbcDF = spark.read()
                    .format("jdbc")
                    .option("url", jdbcUrl)
                    .option("dbtable", table)
                    .option("user", user)
                    .option("password", password)
                    .option("driver", "org.postgresql.Driver")
                    .load();

            // 显示数据
            jdbcDF.show();

            // 或者注册为临时视图后使用SQL查询
            jdbcDF.createOrReplaceTempView("my_table");
            
            Dataset<Row> sqlDF = spark.sql("SELECT * FROM my_table LIMIT 10"); // 修改了查询条件
            sqlDF.show();
        } finally {
            // 确保SparkSession被停止
            spark.stop();
        }
    }
}