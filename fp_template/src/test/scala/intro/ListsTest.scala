package intro

import intro.Lists.customAverage
import org.scalatest.FunSuite

class ListsTest extends FunSuite {

    test("Example") {
        assertResult(5) {
            // 4 + 3 + 6 + 8 + 4 = 25, 25/5 = 5
            customAverage(List(4, 1, 3, 2, 6, 2, 8, 2, 1, 4), 2, 5)
        }
    }

    test("not enough elements above the value") {
        assertResult(0) {
            customAverage(List(1, 2, 1), 2, 10)
        }
    }

    test("take 0 elem") {
        assertResult(0) {
            customAverage(List(1, 2, 1), 0, 0)
        }
    }

    test("different nr between n and filtered.length") {
        assertResult(7) {
            customAverage(List(5, 5, 2, 3, 11), 4, 3)
        }
    }
}
