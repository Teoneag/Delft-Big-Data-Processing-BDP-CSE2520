package intro

import org.scalatest.FunSuite
import intro.PatternMatching2._

class PatternMatching2Test extends FunSuite{


  test(" twice") {
    assertResult(List("abc", "abc")) {
      twice(List("abc"))
    }
  }

  test("twice 0 el") {
    assertResult(List()) {
      twice(List())
    }
  }

  test("twice 2 el") {
    assertResult(List("a", "a", "b", "b")) {
      twice(List("a", "b"))
    }
  }


  test("drunkWords") {
    assertResult(List("uoy","yeh")) {
      drunkWords(List("hey", "you"))
    }
  }

  test("myForAll") {
    assertResult(true) {
      myForAll(List(2, 4, 6), (e: Int) => e % 2 == 0)
    }
  }

  test("myForAll one wrong") {
    assertResult(false) {
      myForAll(List(2, 4, 1, 6), (e: Int) => e % 2 == 0)
    }
  }

  test("myForAll all wrong") {
    assertResult(false) {
      myForAll(List(1, 3), (e: Int) => e % 2 == 0)
    }
  }

  test("myForAll empty") {
    assertResult(true) {
      myForAll(List(), (e: Int) => e % 2 == 0)
    }
  }

  test("lastElem") {
    assertResult(Some("yes")) {
      lastElem(List("no", "yes", "no", "no", "yes"))
    }
  }

  test("lastElem only one el") {
    assertResult(Some("yes")) {
      lastElem(List("yes"))
    }
  }

  test("lastElem 0 el") {
    assertResult(None) {
      lastElem(List())
    }
  }

  test("append") {
    assertResult(List(1,3,5,2,4)) {
      append(List(1,3,5), List(2,4))
    }
  }

  test("append empty first") {
    assertResult(List(2,4)) {
      append(List(), List(2,4))
    }
  }

  test("append empty second") {
    assertResult(List(1,3,5)) {
      append(List(1,3,5), List())
    }
  }
}
