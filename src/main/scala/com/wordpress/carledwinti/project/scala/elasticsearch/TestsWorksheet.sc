import java.time.LocalDateTime
import java.time.format.{DateTimeFormatter, FormatStyle}
import java.util.Locale

val dtf: DateTimeFormatter = DateTimeFormatter.ofPattern("ddMMyyyyHHmmss")
val now = LocalDateTime.now()
println(now.format(dtf))

println("{" +
  "\"settings\":{" +
  "\"number_of_shards\":3," +
  "\"number_of_replicas\":2" +
  "}," +
  "\"mappings\":{" +
  "\"properties\":{" +
  "\"modelo\":{\"type\":\"text\"}," +
  "\"marca\":{\"type\":\"text\"}," +
  "\"anof\":{\"type\":\"integer\"}," +
  "\"xanom\":{\"type\":\"integer\"}," +
  "\"valor\":{\"type\":\"double\"}" +
  "}" +
  "}" +
  "}")

