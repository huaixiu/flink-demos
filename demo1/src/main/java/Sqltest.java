import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

public class Sqltest {
    public Sqltest(){}

    public static void main(String[] args) throws Exception {

        /*TypeInformation[] fieldTypes = new TypeInformation[] {
                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
        };
        String[] fieldsname = {"name" , "age"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldsname);
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://10.180.171.226:3306/flink")
                .setUsername("root")
                .setPassword("123456")
                .setQuery("select * from people")
                .setRowTypeInfo(rowTypeInfo)
                .finish();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource s = env.createInput(jdbcInputFormat); // datasource
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
        tableEnv.registerDataSet("people", s);
        tableEnv.sqlQuery("select * from people").printSchema();
        Table query = tableEnv.sqlQuery("select * from people");
        DataSet result = tableEnv.toDataSet(query, People.class);
        result.print();
        System.out.println(s.count());*/

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<People> dataStream =  env.addSource(new JdbcReader());
        tEnv.registerDataStream("people", dataStream, "name, age");
        Table result = tEnv.sqlQuery("SELECT * FROM people WHERE age = '16' ");
        tEnv.toAppendStream(result, People.class).print();

        try {
            env.execute();
        } catch (Exception var8) {
            var8.printStackTrace();
        }
    }
    public static class People{
        public String name;
        public String age;

        public People(){ }
        public People(String name,String age){
            this.name=name;
            this.age=age;
        }
        public String toString() {
            return "people{name=" + this.name + ", age=" + this.age + '}';
        }

    }

}