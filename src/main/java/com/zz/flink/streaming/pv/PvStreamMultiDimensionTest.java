//package streaming.pv;
//
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.zz.flink.common.model.PageView;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.collector.selector.OutputSelector;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.SplitStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Properties;
//
//public class PvStreamMultiDimensionTest {
//
//	public static void main(String[] args) throws Exception {
//		final ObjectMapper mapper = new ObjectMapper();
//		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		Properties properties = new Properties();
//		properties.setProperty("bootstrap.servers", "localhost:9092");
//// only required for Kafka 0.8
////		properties.setProperty("zookeeper.connect", "localhost:2181");
//		properties.setProperty("group.id", "pvtest");
//		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("pv",new SimpleStringSchema(),properties);
//		DataStreamSource<String> stream = env.addSource(consumer);
//		final List<String> stats = new ArrayList<>();
//		stats.add("page");
//		stats.add("user");
//		SingleOutputStreamOperator<PageView> pvStream = stream.map(new MapFunction<String, PageView>() {
//			@Override
//			public PageView map(String s) throws Exception {
//				return mapper.readValue(s, PageView.class);
//			}
//		});
//		SplitStream<PageView> splitStream = pvStream.split(new OutputSelector<PageView>() {
//			@Override
//			public Iterable<String> select(PageView value) {
//				return stats;
//			}
//		});
//		splitStream.select("page").filter(new FilterFunction<PageView>() {
//			@Override
//			public boolean filter(PageView value) throws Exception {
//				return value.getPageId().endsWith("0");
//			}
//		}).map(new MapFunction<PageView, Tuple2<String,Integer>>() {
//			@Override
//			public Tuple2<String, Integer> map(PageView pageView) throws Exception {
//				return new Tuple2<>(pageView.getPageId(),1);
//			}
//		}).keyBy(0).sum(1)
//				.print().setParallelism(1);
//		splitStream.select("user").filter(new FilterFunction<PageView>() {
//			@Override
//			public boolean filter(PageView value) throws Exception {
//				return value.getUserId().endsWith("0");
//			}
//		}).map(new MapFunction<PageView, Tuple2<String,Integer>>() {
//			@Override
//			public Tuple2<String, Integer> map(PageView pageView) throws Exception {
//				return new Tuple2<>(pageView.getUserId(),1);
//			}
//		}).keyBy(0).sum(1)
//				.print().setParallelism(1);
//		env.execute("kafkaTest");
//	}
//}
