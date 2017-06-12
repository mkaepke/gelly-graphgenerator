package GraphGenerator;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Random;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.types.NullValue;

/**
 * Created by marc on 12.06.17.
 */
public class EdgeValueTranslator implements TranslateFunction<NullValue, Double> {

  private static final long serialVersionUID = -8053698922491887464L;
  private Random random;

  public EdgeValueTranslator() {
    random = new Random();
  }

  /**
   * Erzeugt zufällige Gleitkommazahlen mit zwei Nachkommastellen.
   *
   * @throws Exception
   */
  @Override
  public Double translate(NullValue value, Double reuse) throws Exception {
    /** definierte einen Bereich von 0..100 **/
    Double tmp = random.nextDouble() * (100d - 0d) + 0d;
    /** reduziert alle Double auf zwei Vorkomma- und zwei Nachkommastellen **/
    DecimalFormat df = new DecimalFormat("##.##");
    df.setRoundingMode(RoundingMode.FLOOR);
    return Double.parseDouble(df.format(tmp).replace(',', '.'));
  }
}
