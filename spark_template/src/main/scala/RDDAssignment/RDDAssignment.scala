package RDDAssignment

import java.util.UUID
import java.math.BigInteger
import java.security.MessageDigest
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import utils.{Commit, File, Stats}

import java.sql.Timestamp

object RDDAssignment {


  /**
   * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
   * we want to know how many commits a given RDD contains.
   *
   * @param commits RDD containing commit data.
   * @return Long indicating the number of commits in the given RDD.
   */
  def assignment_1(commits: RDD[Commit]): Long = commits.count()

  /**
   * We want to know how often programming languages are used in committed files. We want you to return an RDD containing Tuples
   * of the used file extension, combined with the number of occurrences. If no filename or file extension is used we
   * assume the language to be 'unknown'.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing tuples indicating the programming language (extension) and number of occurrences.
   */
  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = commits
    .flatMap(_.files)
    .flatMap(_.filename)
    .map(s => if (s.contains('.')) s.split('.').last else "unknown")
    .map((_, 1L))
    .reduceByKey(_ + _)

  /**
   * Competitive users on GitHub might be interested in their ranking in the number of commits. We want you to return an
   * RDD containing Tuples of the rank (zero indexed) of a commit author, a commit author's name and the number of
   * commits made by the commit author. As in general with performance rankings, a higher performance means a better
   * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
   * tie.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing the rank, the name and the total number of commits for every author, in the ordered fashion.
   */
  def assignment_3(commits: RDD[Commit]): RDD[(Long, String, Long)] = commits
    .map(c => c.commit.author.name)
    .map((_, 1L))
    .reduceByKey(_ + _)
    .sortBy { case (name, count) => (-count, name.toLowerCase()) }
    .zipWithIndex()
    .map { case ((name, count), index) => (index, name, count) }

  /**
   * Some users are interested in seeing an overall contribution of all their work. For this exercise we want an RDD that
   * contains the committer's name and the total number of their commit statistics. As stats are Optional, missing Stats cases should be
   * handled as "Stats(0, 0, 0)".
   *
   * Note that if a user is given that is not in the dataset, then the user's name should not occur in
   * the resulting RDD.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing committer names and an aggregation of the committers Stats.
   *         5:22 -> 52
   */
  def assignment_4(commits: RDD[Commit], users: List[String]): RDD[(String, Stats)] = {
    val userSet = users.toSet

    commits
      .filter(commit => userSet.contains(commit.commit.author.name))
      .map(commit => (commit.commit.author.name, commit.stats.getOrElse(Stats(0, 0, 0))))
      .reduceByKey((a, b) => Stats(a.total + b.total, a.additions + b.additions, a.deletions + b.deletions))
  }

  /**
   * There are different types of people: those who own repositories, and those who make commits. Although Git blame command is
   * excellent in finding these types of people, we want to do it in Spark. As the output, we require an RDD containing the
   * names of commit authors and repository owners that have either exclusively committed to repositories, or
   * exclusively own repositories in the given commits RDD.
   *
   * Note that the repository owner is contained within GitHub URLs.
   *
   * @param commits RDD containing commit data.
   * @return RDD of Strings representing the usernames that have either only committed to repositories or only own
   *         repositories.
   *         5:52 -> 6:12
   */
  def assignment_5(commits: RDD[Commit]): RDD[String] = commits
    .flatMap(c => List((c.commit.author.name, "author"), (c.url.split("/")(4), "owner"))) // "name1" -> "author", name2 -> "owner"
    .distinct()
    .groupByKey() // each name -> list of roles
    .filter(_._2.size == 1) // a name is either an author or an owner, not both
    .map(_._1) // name

  /**
   * Sometimes developers make mistakes and sometimes they make many many of them. One way of observing mistakes in commits is by
   * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
   * in a commit message. Note that for a commit to be eligible for a 'revert streak', its message must start with `Revert`.
   * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
   * would not be a 'revert streak' at all.
   *
   * We require an RDD containing Tuples of the username of a commit author and a Tuple containing
   * the length of the longest 'revert streak' of a user and how often this streak has occurred.
   * Note that we are only interested in the longest commit streak of each author (and its frequency).
   *
   * @param commits RDD containing commit data.
   * @return RDD of Tuples containing a commit author's name and a Tuple which contains the length of the longest
   *         'revert streak' as well its frequency.
   */
  def assignment_6(commits: RDD[Commit]): RDD[(String, (Int, Int))] = commits
    .filter(c => c.commit.message.startsWith("Revert")) // only relevant commits
    .map(c => (c.commit.author.name, c.commit.message.split("Revert").length - 1)) // author -> streak
    .groupByKey() // author -> list of streaks
    .mapValues(iter => // count nr of longest streaks, its nr of occurrences
      iter.foldLeft((0, 0)) { case ((max, count), streak) =>
        if (streak > max) (streak, 1)
        else if (streak == max) (max, count + 1)
        else (max, count)
      }
    )

  /**
   * !!! NOTE THAT FROM THIS EXERCISE ON (INCLUSIVE), EXPENSIVE FUNCTIONS LIKE groupBy ARE NO LONGER ALLOWED TO BE USED !!
   *
   * We want to know the number of commits that have been made to each repository contained in the given RDD. Besides the
   * number of commits, we also want to know the unique committers that contributed to each of these repositories.
   *
   * In real life these wide dependency functions are performance killers, but luckily there are better performing alternatives!
   * The automatic graders will check the computation history of the returned RDDs.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing Tuples with the repository name, the number of commits made to the repository as
   *         well as the names of the unique committers to this repository.
   */
  def assignment_7(commits: RDD[Commit]): RDD[(String, Long, Iterable[String])] = commits
    .map(c => (repo(c), c.commit.committer.name)) // repository -> commiters
    .aggregateByKey((0L, Set[String]()))( // (initial nr of commits, initial set of commiters)
      (acc, author) => (acc._1 + 1, acc._2 + author), // add one commit and commiters to the set
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 ++ acc2._2) // combine two (nr of commits, commiters) tuples
    )
    .map(t => (t._1, t._2._1, t._2._2)) // from (repository, (nr of commits, authors)) to (repository, nr of commits, commiters)

  private def repo(c: Commit): String = c.url.split("/")(5)

  /**
   * Return an RDD of Tuples containing the repository name and all the files that are contained in this repository.
   * Note that the file names must be unique, so if a file occurs multiple times (for example, due to removal, or new
   * addition), the newest File object must be returned. As the filenames are an `Option[String]`, discard the
   * files that do not have a filename.
   *
   * To reiterate, expensive functions such as groupBy are not allowed.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing the files in each repository as described above.
   */
  // ToDo the newest file check
  // c.commit.committer.date
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[File])] = {
    def addFilesWithNewestDate(existingFiles: Set[(File, Timestamp)], newFiles: Set[(File, Timestamp)]): Set[(File, Timestamp)] = {
      newFiles.map { case (newFile, newDate) =>
        existingFiles.find { case (existingFile, _) => existingFile.filename == newFile.filename } match {
          case Some((existingFile, existingDate)) if existingDate.after(newDate) => (existingFile, existingDate)
          case _ => (newFile, newDate)
        }
      } ++ existingFiles.filter { case (existingFile, _) =>
        newFiles.forall { case (newFile, _) => existingFile.filename != newFile.filename }
      }
    }

    commits
      .map(c => (repo(c), c.files.map((_, c.commit.committer.date)))) // repository -> files
      .aggregateByKey(Set[(File, Timestamp)]())( // initial set of files
        (acc, filesWithDates) => addFilesWithNewestDate(acc, filesWithDates.toSet), // Add files with date comparison
        (acc1, acc2) => addFilesWithNewestDate(acc1, acc2) // Merge partitions with date comparison
      )
      .map(t => (t._1, t._2.map(_._1))) // from (repository, files) to (repository, files (but iterable))
  }

  /**
   * For this assignment you are asked to find all the files of a single repository. This is in order to create an
   * overview of each file by creating a Tuple containing the file name, all corresponding commit SHA's,
   * as well as a Stat object representing all the changes made to the file.
   *
   * To reiterate, expensive functions such as groupBy are not allowed.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing Tuples representing a file name, its corresponding commit SHA's and a Stats object
   *         representing the total aggregation of changes for a file.
   */
  def assignment_9(commits: RDD[Commit], repository: String): RDD[(String, Seq[String], Stats)] = {
    def addStats(s1: Stats, s2: Stats): Stats = Stats(s1.total + s2.total, s1.additions + s2.additions, s1.deletions + s2.deletions)

    commits
      .filter(c => repo(c) == repository) // only commits of the given repository
      .flatMap(c => c.files.map(f => (f.filename, c.sha, Stats(f.changes, f.additions, f.deletions)))) // Option(filename) -> (sha, stats)
      .collect { case (Some(filename), sha, stats) => (filename, (sha, stats)) } // only files with a filename
      .aggregateByKey((Seq[String](), Stats(0L, 0L, 0L)))( // initial (shas, stats)
        (acc, t) => (acc._1 :+ t._1, addStats(acc._2, t._2)), // add (sha, stats) to tuple
        (acc1, acc2) => (acc1._1 ++ acc2._1, addStats(acc1._2, acc2._2)) // combine two (shas, stats) tuples
      )
      .map(t => (t._1, t._2._1, t._2._2)) // from (filename, (shas, stats)) to (filename, shas, stats)
  }

  /**
   * We want to generate an overview of the work done by a user per repository. For this we want an RDD containing
   * Tuples with the committer's name, the repository name and a `Stats` object containing the
   * total number of additions, deletions and total contribution to this repository.
   * Note that since Stats are optional, the required type is Option[Stat].
   *
   * To reiterate, expensive functions such as groupBy are not allowed.
   *
   * @param commits RDD containing commit data.
   * @return RDD containing Tuples of the committer's name, the repository name and an `Option[Stat]` object representing additions,
   *         deletions and the total contribution to this repository by this committer.
   */
  def assignment_10(commits: RDD[Commit]): RDD[(String, String, Option[Stats])] = {
    def combineStats(optStats: Option[Stats], newStats: Option[Stats]): Option[Stats] = {
      (optStats, newStats) match {
        case (None, s) => s
        case (Some(s1), Some(s2)) => Some(Stats(s1.total + s2.total, s1.additions + s2.additions, s1.deletions + s2.deletions))
        case (Some(s), None) => Some(s)
      }
    }

    commits
      .map(c => ((c.commit.committer.name, repo(c)), c.stats)) // (name, repo) -> Option(stats)
      .aggregateByKey(None: Option[Stats])( // initial None
        (optStats, newStats) => combineStats(optStats, newStats), // add stat to acc
        (accumulatedStats, partitionStats) => combineStats(accumulatedStats, partitionStats) // combine two Option[Stats]
      )
      .map(t => (t._1._1, t._1._2, t._2))
  }

  /**
   * Hashing function that computes the md5 hash of a String and returns a Long, representing the most significant bits of the hashed string.
   * It acts as a hashing function for repository name and username.
   *
   * @param s String to be hashed, consecutively mapped to a Long.
   * @return Long representing the MSB of the hashed input String.
   */
  def md5HashString(s: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    UUID.nameUUIDFromBytes(hashedString.getBytes()).getMostSignificantBits
  }

  /**
   * Create a bi-directional graph from committer to repositories. Use the `md5HashString` function above to create unique
   * identifiers for the creation of the graph.
   *
   * Spark's GraphX library is actually used in the real world for algorithms like PageRank, Hubs and Authorities, clique finding, etc.
   * However, this is out of the scope of this course and thus, we will not go into further detail.
   *
   * We expect a node for each repository and each committer (based on committer name).
   * We expect an edge from each committer to the repositories that they have committed to.
   *
   * Look into the documentation of Graph and Edge before starting with this exercise.
   * Your vertices must contain information about the type of node: a 'developer' or a 'repository' node.
   * Edges must only exist between repositories and committers.
   *
   * To reiterate, expensive functions such as groupBy are not allowed.
   *
   * @param commits RDD containing commit data.
   * @return Graph representation of the commits as described above.
   */
  def assignment_11(commits: RDD[Commit]): Graph[(String, String), String] = {
    def extractRepoFull(str: String): String = str.replaceAll(".*/repos/([^/]*/[^/]*)/.*", "$1")

    Graph(commits
      .map(c => (md5HashString(c.commit.committer.name), (c.commit.committer.name, "developer")))
      .distinct().union(commits
        .map(c => (md5HashString(extractRepoFull(c.url)), (extractRepoFull(c.url), "repository")))
        .distinct()), commits
      .map(c => Edge(md5HashString(extractRepoFull(c.url)), md5HashString(c.commit.committer.name), "edge"))
      .distinct())
  }
}
