package dataset

import Dataset._
import dataset.util.Commit.Commit
import org.json4s._
import org.json4s.native.Serialization
import org.json4s.native.Serialization._
import org.scalatest.FunSuite

import scala.io.Source

class DatasetTest extends FunSuite {

    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    val source: List[Commit] = Source.fromResource("1000_commits.json").getLines().map(Serialization.read[Commit]).toList

    test("Average additions"){
        assertResult(3137) {
            avgAdditions(source)
        }
    }

    test("Average additions with 0 good commits") {
        assertResult(0) {
            avgAdditions(List())
        }
    }

    test("Time of day javascript") {
        assertResult((12, 830)) {
            jsTime(source)
        }
    }

    test("Top committer") {
        assertResult(("Leonid Plyushch", 12)) {
            topCommitter(source, "termux/termux-packages")
        }
    }

    test("Commits per repo") {
        assertResult(read[Map[String, Int]](Source.fromResource("commits_per_repo.json").mkString)) {
            commitsPerRepo(source)
        }
    }

    test("Top languages") {
        assertResult(List(("html",910), ("js",848), ("json",554), ("png",434), ("md",408))) {
            topFileFormats(source)
        }
    }

    test("mostProductivePart") {
        assertResult(("afternoon", 992)) {
            mostProductivePart(source)
        }
    }
}
