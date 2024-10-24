package fp_practice

import FPPractice._
import org.scalatest.FunSuite

class FPPracticeTest extends FunSuite {

    test("First 10") {
        assertResult(305) {
            first10Above25(List.range(0, 40))
        }
    }

    test("Passing grades") {
        val in = List(
            List(5, 10, 6, 7, 8, 4),
            List(9, 3, 5, 6, 7, 5),
            List(5, 8, 7, 9, 6, 10),
            List(5, 5, 5, 6, 6, 5)
        )
        assertResult(2) {
            passingStudents(in)
        }
    }

    test("Head sums tail") {
        val in = List(
            List(5, 4, 3, 2, 1, 6),
            List(10, 4, 3, 2, 1),
            List(4, 1, 2, -1),
        )
        assertResult(Some(5)) {
            headSumsTail(in)
        }
    }

    test("Head sums tail with empty lists") {
        val in = List(
            List(),
            List(5, 4, 3, 2, 1, 6),
            List(10, 4, 3, 2, 1),
            List(4, 1, 2, -1),
        )
        assertResult(Some(5)) {
            headSumsTail(in)
        }
    }
}
