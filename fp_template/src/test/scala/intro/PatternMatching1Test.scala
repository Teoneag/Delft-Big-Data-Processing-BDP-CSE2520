package intro

import intro.PatternMatching1._
import org.scalatest.FunSuite

class PatternMatching1Test extends FunSuite {

    test("Sum") {
        assertResult(15) {
            sum((1 to 5).toList)
        }
    }

    test("Optional sum") {
        assertResult(16) {
            optionalSum(List(Num(2), Nothing(), Num(5), Num(3), Nothing(), Num(2), Nothing(), Nothing(), Num(4)))
        }
    }

    test("FirstDivBy") {
        assertResult(Num(8)) {
            firstDivByX(List(6, 13, 8, 10, 12), 4)
        }
    }

    test("FirstDivBy with no nr div by that") {
        assertResult(Nothing()) {
            firstDivByX(List(6, 13, 8, 10, 12), 69)
        }
    }

    test("EvenNumbers") {
        assertResult(List(2, 4, 6, 8, 10)) {
            onlyEvenNumbers(List(Num(1), Num(2), Nothing(), Num(3), Num(4), Nothing(), Nothing(), Num(5), Nothing(), Num(6),
                Num(7), Num(8), Nothing(), Num(9), Nothing(), Nothing(), Num(10)))
        }
    }

    test("EvenNumbers empty result") {
        assertResult(List()) {
            onlyEvenNumbers(List(Num(1), Num(3), Nothing(), Num(3), Num(7), Nothing(), Nothing(), Num(5), Nothing(), Num(7),
                Num(7), Num(9), Nothing(), Num(9), Nothing(), Nothing(), Num(11)))
        }
    }
}
