package GraphGenerator;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.graph.generator.random.RandomGenerableFactory;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by marc on 12.06.17.
 */
public class RMatGenerator implements Serializable {

  private static final long serialVersionUID = -3638369167443027404L;

  private Logger log = LoggerFactory.getLogger("RMatGenerator.class");

  private final ExecutionEnvironment env;
  private Graph<Double, NullValue, Double> graph;

  /**
   *
   * @param env
   */
  public RMatGenerator(ExecutionEnvironment env) {
    this.env = env;
  }

  /**
   * Generiert einen power-law Graphen mittels {@link RMatGraph}.
   *
   * @param scale, skaliert die Knotenanzahl
   * @param edgefactor, Faktor für die Kantenanzahl je Knoten
   * @return graph, generierte Graph als Rückgabe
   */
  public Graph<Double, NullValue, Double> generate(int scale, int edgefactor) throws Exception {
    RandomGenerableFactory<JDKRandomGenerator> rnd = new JDKRandomGeneratorFactory();

    long vertexCount = 1 << scale;
    long edgeCount = vertexCount * edgefactor;

    Graph<LongValue, NullValue, NullValue> initialGraph = new RMatGraph<>(env, rnd, vertexCount, edgeCount)
        .generate();

    graph = initialGraph.translateEdgeValues(new EdgeValueTranslator()).translateGraphIds(new GraphIdTranslator());
    log.info("power-law graph generated with {} vertices and {} edges.", graph.numberOfVertices(), graph.numberOfEdges());
    return graph;
  }

  /**
   * Generiert einen power-law Graphen mittels {@link #generate(int, int)} und schreibt den Graphen als CSV.
   *
   * @param scale, skaliert die Knotenanzahl
   * @param edgefactor, Faktor für die Kantananzahl
   * @param outputPath, Pfad für die Knoten- & Kanten-CSV
   * @throws Exception
   */
  public void generateAndWrite(int scale, int edgefactor, String outputPath) throws Exception {
    generate(scale, edgefactor);

    writeCsv(outputPath);
  }

  /**
   * Generiert einen power-law Graphen mittels {@link #generate(int, int)} und schreibt den Graphen als CSV.
   * Die Knoten- & Kanten-CSV werden ins Verzeichnis ./generatedGraph/ geschrieben.
   *
   * @param scale, skaliert die Knotenanzahl
   * @param edgefactor, Faktor für die Kantananzahl
   * @throws Exception
   */
  public void generateAndWrite(int scale, int edgefactor) throws Exception {
    generate(scale, edgefactor);

    String outputPath = "./generatedGraph/";

    writeCsv(outputPath);
  }

  /**
   * Hilfsmethode zum Schreiben der CSV Dateien.
   * Bestehende Dateien werden überschrieben.
   *
   * @param outputPath
   */
  private void writeCsv(String outputPath) throws Exception {
    DataSet<Vertex<Double, NullValue>> vertexDataSet = graph.getVertices();
    DataSet<Edge<Double, Double>> edgeDataSet = graph.getEdges();

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd_HH:mm");
    Date date = new Date();
    if(!outputPath.endsWith("/")) {
      outputPath = outputPath.concat("/");
    }
    outputPath = outputPath.concat(sdf.format(date) + "/");

    vertexDataSet
        .writeAsCsv(outputPath + "vertices_" + graph.numberOfVertices() + ".csv", WriteMode.OVERWRITE)
        .name("writeGeneratedVertexCSV")
        .setParallelism(1);
    edgeDataSet
        .writeAsCsv(outputPath + "edges_" + graph.numberOfEdges() + ".csv", WriteMode.OVERWRITE)
        .name("writeGeneratedEdgeCSV")
        .setParallelism(1);

    try {
      env.execute("write CSV");
      log.info("writing process was successful");
    } catch (Exception e) {
      log.error("couldn't write generated graph", e);
    }
  }
}
