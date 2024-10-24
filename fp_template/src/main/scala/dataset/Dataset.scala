package dataset

import dataset.util.Commit.{Commit, File}

import java.text.SimpleDateFormat
import java.util.TimeZone

/**
 * Use your knowledge of functional programming to complete the following functions.
 * You are recommended to use library functions when possible.
 *
 * The data is provided as a list of `Commit`s. This case class can be found in util/Commit.scala.
 * When asked for dates, use the `commit.commit.committer.date` field.
 *
 * This part is worth 40 points.
 */
object Dataset {


  /** Q23 (4p)
   * For the commits that are accompanied with stats data, compute the average of their additions.
   * You can assume a positive amount of usable commits is present in the data.
   *
   * @param input the list of commits to process.
   * @return the average amount of additions in the commits that have stats data.
   */
  def avgAdditions(input: List[Commit]): Int = {
    val additions = input.flatMap(_.stats.map(_.additions))
    if (additions.isEmpty) 0 else additions.sum / additions.size
  }

  /** Q24 (4p)
   * Find the hour of day (in 24h notation, UTC time) during which the most javascript (.js) files are changed in commits.
   * The hour 00:00-00:59 is hour 0, 14:00-14:59 is hour 14, etc.
   * NB!filename of a file is always defined.
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   *
   * @param input list of commits to process.
   * @return the hour and the amount of files changed during this hour.
   */
  def jsTime(input: List[Commit]): (Int, Int) = {
    val hoursWithNrJsFiles = input.map(commit => (
      getHour(commit),
      commit.files.count((x: File) => x.filename.get.endsWith(".js")) // nr of js files of the commit
    ))

    val fileCountByHour = hoursWithNrJsFiles.groupBy(_._1).mapValues(_.map(_._2).sum)

    fileCountByHour.maxBy(_._2)
  }

  private def getHour(commit: Commit): Int = {
    val sdf = new SimpleDateFormat("HH") // for 24-hour format
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
    sdf.format(commit.commit.committer.date).toInt
  }

  /** Q25 (5p)
   * For a given repository, output the name and amount of commits for the person
   * with the most commits to this repository.
   * For the name, use `commit.commit.author.name`.
   *
   * @param input the list of commits to process.
   * @param repo  the repository name to consider.
   * @return the name and amount of commits for the top committer.
   */

  def topCommitter(input: List[Commit], repo: String): (String, Int) = {
    val commits = input.filter(commit => getRepo(commit) == repo)

    val names = commits.map(commit => commit.commit.author.name)

    val nameCounts = names.groupBy(identity).mapValues(_.size)

    nameCounts.maxBy(_._2)
  }

  private def getRepo(commit: Commit): String = {
    val repoPattern = """/repos/([^/]+/[^/]+)/commits""".r
    repoPattern.findFirstMatchIn(commit.url).get.group(1)
  }

  /** Q26 (9p)
   * For each repository, output the name and the amount of commits that were made to this repository in 2019 only.
   * Leave out all repositories that had no activity this year.
   *
   * @param input the list of commits to process.
   * @return a map that maps the repo name to the amount of commits.
   *
   *         Example output:
   *         Map("KosDP1987/students" -> 1, "giahh263/HQWord" -> 2)
   */
  def commitsPerRepo(input: List[Commit]): Map[String, Int] = {
    val sdf = new SimpleDateFormat("yyyy") // for 24-hour format
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"))

    val commits2019 = input.filter(commit => sdf.format(commit.commit.committer.date).toInt == 2019)

    val repos = commits2019.map(getRepo)

    repos.groupBy(identity).mapValues(_.size)
  }


  /** Q27 (9p)
   * Derive the 5 file types that appear most frequent in the commit logs.
   * NB!filename of a file is always defined.
   *
   * @param input the list of commits to process.
   * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
   */
  def topFileFormats(input: List[Commit]): List[(String, Int)] = {
    def getFileType(file: File): String = {
      if (file.filename.isEmpty) return ""
      """\.(\w+)$""".r.findFirstMatchIn(file.filename.get).map(_.group(1)).getOrElse("")
    }

    val fileTypes = input.flatMap(commit => commit.files.map(getFileType).filter(_.nonEmpty))

    val fileTypesCounts = fileTypes.groupBy(identity).mapValues(_.size)

    fileTypesCounts.toList.sortBy(-_._2).take(5)
  }


  /** Q28 (9p)
   *
   * A day has different parts:
   * morning 5 am to 12 pm (noon)
   * afternoon 12 pm to 5 pm.
   * evening 5 pm to 9 pm.
   * night 9 pm to 4 am.
   *
   * Which part of the day was the most productive in terms of commits ?
   * Return a tuple with the part of the day and the number of commits
   *
   * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
   */
  def mostProductivePart(input: List[Commit]): (String, Int) = {
    def getPartOfDay(commit: Commit): String = getHour(commit) match {
      case h if h >= 5 && h < 12 => "morning"
      case h if h >= 12 && h < 17 => "afternoon"
      case h if h >= 17 && h < 21 => "evening"
      case _ => "night"
    }

    val partOfDay = input.map(getPartOfDay)

    val partOfDayCounts = partOfDay.groupBy(identity).mapValues(_.size)

    partOfDayCounts.maxBy(_._2)
  }
}