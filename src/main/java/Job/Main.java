package Job;

import GraphGenerator.RMatGenerator;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by marc on 12.06.17.
 */
public class Main {

  public static void main(String[] args) {
    Logger log = LoggerFactory.getLogger("Main.class");

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);

    RMatGenerator generator = new RMatGenerator(env);
    try {
      int scale = params.getInt("scale", 50);
      int edgefactor = params.getInt("factor", scale / 10);
      String path = params.getRequired("path");

      generator.generateAndWrite(scale, edgefactor, path);
    } catch (Exception e) {
      log.error("Error while parsing args", e);
    }
  }
}
