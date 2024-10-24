package intro

import Practice._
import org.scalatest.FunSuite

class PracticeTest extends FunSuite {

    test("FirstN") {
        assertResult((1 to 10).toList) {
            firstN((1 to 20).toList, 10)
        }
    }

    test("FirstN where list has less than n el") {
        assertResult((1 to 20).toList) {
            firstN((1 to 20).toList, 25)
        }
    }

    test("FirstN with 0 el") {
        assertResult(List()) {
            firstN(List(), 25)
        }
    }

    test("First 0 el") {
        assertResult(List()) {
            firstN((1 to 20).toList, 0)
        }
    }

    test("MaxValue") {
        assertResult(16) {
            maxValue(List(10, 4, 14, -4, 15, 14, 16, 7))
        }
    }

    test("MaxValue 1 el") {
        assertResult(16) {
            maxValue(List(16))
        }
    }

    test("MaxValue 0 el") {
        assertResult(Int.MinValue) {
            maxValue(List())
        }
    }

    test("intList") {
        assertResult(List(1, 2, 3, 4, 5)) {
            intList(1, 5)
        }
    }


    test("intList one nr") {
        assertResult(List(1)) {
            intList(1, 1)
        }
    }

    test("intList negative nr of nrs") {
        assertResult(List()) {
            intList(2, 1)
        }
    }

    test("myFilter") {
        assertResult(List(0, 4, 8)) {
            myFilter((0 to 10).toList, (x: Int) => x % 2 == 0)
        }
    }
}
