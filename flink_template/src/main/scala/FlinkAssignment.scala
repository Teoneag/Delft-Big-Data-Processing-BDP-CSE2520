import java.text.SimpleDateFormat
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.CEP
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary, Stats}
import util.{CommitGeoParser, CommitParser}

import java.util
import java.util.Date


/** Do NOT rename this class, otherwise autograding will fail. * */
object FlinkAssignment {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
     * Setups the streaming environment including loading and parsing of the datasets.
     *
     * DO NOT TOUCH!
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    //    dummy_question(commitStream).print()
    //    question_one(commitStream).print()
    //    question_two(commitStream).print()
    //    question_three(commitStream).print()
    //    question_four(commitStream).print()
    //    question_five(commitStream).print()
    //    question_six(commitStream).print()
    //    question_seven(commitStream).print()
    //    question_eight(commitStream, commitGeoStream).print()
    question_nine(commitStream).print()


    /** Start the streaming environment. * */
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
   * Write a Flink application which outputs the sha of commits with at least 20 additions.
   * Output format: sha
   */
  def question_one(input: DataStream[Commit]): DataStream[String] = input
    .filter(_.stats.exists(_.additions >= 20))
    .map(_.sha)

  /**
   * Write a Flink application which outputs the names of the files with more than 30 deletions.
   * Output format:  fileName
   */
  def question_two(input: DataStream[Commit]): DataStream[String] = input
    .flatMap(_.files.filter(_.deletions > 30))
    .filter(_.filename.isDefined)
    .map(_.filename.get)

  /**
   * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
   * Output format: (fileExtension, #occurrences)
   */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = input
    .flatMap(_.files.filter(file => file.filename.exists(name => name.endsWith(".java") || name.endsWith(".scala"))))
    .map(file => (file.filename.get.split("\\.").last, 1))
    .keyBy(_._1)
    .sum(1)

  /**
   * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
   * Output format: (extension, status, count)
   */
  def question_four(input: DataStream[Commit]): DataStream[(String, String, Int)] = input
    .flatMap(_.files.filter(file => file.filename.exists(name => name.endsWith(".js") || name.endsWith(".py")))
      .map(file => (file.filename.get.split("\\.").last, file.status.getOrElse("unknown"), file.changes)))
    .keyBy(x => (x._1, x._2))
    .sum(2)

  /**
   * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
   * Make use of a non-keyed window.
   * Output format: (date, count)
   */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input
      .assignAscendingTimestamps(_.commit.committer.date.getTime) // Watermark each el with a Long
      .timeWindowAll(Time.days(1))
      .apply((window: TimeWindow, elements: Iterable[Commit], out: Collector[(String, Int)]) => // count / window
        out.collect((formatDate(new Date(window.getStart)), elements.size))) // date -> count
  }

  private def formatDate(date: Date): String = new SimpleDateFormat("dd-MM-yyyy").format(date)

  /**
   * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
   * Compute every 12 hours the amount of small and large commits in the last 48 hours.
   * Output format: (type, count)
   */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = input
    .map(commit => (
      if (commit.stats.exists(_.total > 20)) "large" else "small", // type
      1, // count
      commit.commit.committer.date.getTime)) // time
    .assignAscendingTimestamps(_._3)
    .keyBy(_._1)
    .window(SlidingEventTimeWindows.of(Time.hours(48), Time.hours(12)))
    .sum(1)
    .map(x => (x._1, x._2))

  /**
   * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
   *
   * The fields of this case class:
   *
   * repo: name of the repo.
   * date: use the start of the window in format "dd-MM-yyyy".
   * amountOfCommits: the number of commits on that day for that repository.
   * amountOfCommitters: the amount of unique committers contributing to the repository.
   * totalChanges: the sum of total changes in all commits.
   * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
   *
   * Hint: Write your own ProcessWindowFunction.
   * Output format: CommitSummary
   */

  def question_seven(commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    commitStream
      .assignAscendingTimestamps(_.commit.committer.date.getTime)
      .keyBy(c => extractRepoFull(c.url))
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .process(new ProcessWindowFunction[Commit, CommitSummary, String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[Commit], out: Collector[CommitSummary]): Unit = {
          val allCommiters = elements.map(_.commit.committer.name)
          val committerFrequency = allCommiters.groupBy(identity).mapValues(_.size)
          val maxCommits = committerFrequency.values.max

          val repo = key
          val date = formatDate(new Date(context.window.getStart))
          val amountOfCommits = elements.size
          val amountOfCommitters = committerFrequency.size
          val totalChanges = elements.map(_.stats.getOrElse(Stats(0, 0, 0)).total).sum
          val mostPopularCommitter: String = committerFrequency.filter(_._2 == maxCommits).keys.toList.sorted.mkString(",")

          if (amountOfCommits > 20 && amountOfCommitters <= 2) {
            out.collect(CommitSummary(repo, date, amountOfCommits, amountOfCommitters, totalChanges, mostPopularCommitter))
          }
        }
      })
  }

  private def extractRepoFull(str: String): String = str.replaceAll(".*/repos/([^/]*/[^/]*)/.*", "$1")

  /**
   * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
   * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
   * Get the weekly amount of changes for the java files (.java extension) per continent. If no java files are changed in a week, no output should be shown that week.
   *
   * Hint: Find the correct join to use!
   * Output format: (continent, amount)
   */
  def question_eight(commitStream: DataStream[Commit], geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {
    commitStream
      .assignAscendingTimestamps(_.commit.committer.date.getTime)
      .keyBy(_.sha)
      .intervalJoin(geoStream.assignAscendingTimestamps(_.createdAt.getTime).keyBy(_.sha))
      .between(Time.hours(-1), Time.minutes(30))
      .process((commit: Commit, commitGeo: CommitGeo, context: ProcessJoinFunction[Commit, CommitGeo, (Commit, String)]#Context, out: Collector[(Commit, String)]) => out.collect((commit, commitGeo.continent)))
      .assignAscendingTimestamps(_._1.commit.committer.date.getTime)
      .keyBy(_._2)
      .window(TumblingEventTimeWindows.of(Time.days(7)))
      .apply((key: String, _: TimeWindow, elements: Iterable[(Commit, String)], out: Collector[(String, Int)]) => {
        val totalChanges = elements.flatMap(_._1.files.filter(_.filename.exists(_.endsWith(".java"))).map(_.changes)).sum
        if (totalChanges > 0) out.collect((key, totalChanges))
      })
  }

  /**
   * Find all files that were added and removed within one day. Output as (repository, filename).
   *
   * Hint: Use the Complex Event Processing library (CEP).
   * Output format: (repository, filename)
   */
  def question_nine(inputStream: DataStream[Commit]): DataStream[(String, String)] = {
    val pattern = Pattern
      .begin[(String, String, Long, String)]("added")
      .where(event => event._4.contains("added"))
      .followedBy("removed")
      .where(event => event._4.contains("removed"))
      .within(Time.days(1))

    val stream = inputStream
      .flatMap(commit => commit.files
        .filter(file => file.filename.isDefined && file.status.isDefined)
        .map(file => (extractRepoFull(commit.url), file.filename.get, commit.commit.committer.date.getTime, file.status.get)))
      .assignAscendingTimestamps(_._3)
      .keyBy(e => (e._1, e._2))

    CEP.pattern(stream, pattern)
      .select(new PatternSelectFunction[(String, String, Long, String), (String, String)] {
        override def select(map: util.Map[String, util.List[(String, String, Long, String)]]): (String, String) = {
          val added = map.get("added").get(0)
          (added._1, added._2)
        }
      })
  }
}
