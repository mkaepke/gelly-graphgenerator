package GraphGenerator;

import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.types.LongValue;

/**
 * Created by marc on 12.06.17.
 */
public class GraphIdTranslator implements TranslateFunction<LongValue, Double> {

  private static final long serialVersionUID = 7496480386646884543L;

  /**
   * Rechnet den LongValue in ein Double um.
   *
   * @throws Exception
   */
  @Override
  public Double translate(LongValue value, Double reuse) throws Exception {
    Long l = value.getValue();
    return l.doubleValue();
  }
}
