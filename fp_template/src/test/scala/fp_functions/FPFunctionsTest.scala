package fp_functions

import FPFunctions._
import org.scalatest.FunSuite

class FPFunctionsTest extends FunSuite {

    test("Map") {
        assertResult((3 to 7).toList) {
            map((1 to 5).toList, (x: Int) => x + 2)
        }
    }

    test("Filter") {
        assertResult(List(2, 4, 6, 8, 10)) {
            filter((1 to 10).toList, (x: Int) => x % 2 == 0)
        }
    }

    test("Flatten") {
        assertResult(List(1, 2, 3, 4)) {
            recFlat(List(List(1), List(2, 3), 4))
        }
    }

    test("FoldL") {
        assertResult(7) {
            foldL(List(1, 5, 3, 6), (x: Int, y: Int) => x + y, -8)
        }
    }

    test("FoldR") {
        assertResult(7) {
            foldR(List(1, 5, 3, 6), (x: Int, y: Int) => x + y, -8)
        }
    }

    test("Zip") {
        assertResult(List((1, 2), (3, 4), (5, 6), (7, 8))) {
            zip(List(1, 3, 5, 7), List(2, 4, 6, 8))
        }
    }
}
