import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.sql.DriverManager;
import com.mysql.jdbc.*;
import java.sql.ResultSet;

public class JdbcReader extends RichSourceFunction<Sqltest.People> {
    private Connection connection = null;
    private PreparedStatement pr = null;

    public void open(Configuration parameters) throws Exception{
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connection = (Connection) DriverManager.getConnection("jdbc:mysql://10.180.171.226:3306/flink", "root", "");
        pr = (PreparedStatement) connection.prepareStatement("select * from people");
    }

    @Override
    public void run(SourceContext<Sqltest.People> sourceContext) throws Exception {
        ResultSet resultSet = pr.executeQuery();
        while (resultSet.next()){
            String name = resultSet.getString("name");
            String age = resultSet.getString("age");
            Sqltest.People people = new Sqltest.People(name, age);
            sourceContext.collect(people);
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (pr != null) {
                pr.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


 }


