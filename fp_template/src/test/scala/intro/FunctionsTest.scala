package intro

import intro.Functions._
import org.scalatest.FunSuite

class FunctionsTest extends FunSuite {

    test("Simple math") {
        // Using `assert` you can make assertions in tests.
        assert(simpleMath(3) == 17, "3 * 5 + 2 = 17")
    }

    test("More math") {
        // Alternatively you can expect a certain result from an operation.
        // This gives better error messages and looks cleaner.
        assertResult(42, "8 * 5 + 2 = 42") {
            simpleMath(8)
        }
    }

    test("Fizz") {
        assertResult("Fizz") {
            fizzBuzz(3)
//            fail("To activate the test, uncomment the previous line and comment this line")
        }
    }

    test("Buzz") {
        assertResult("Buzz") {
            fizzBuzz(5)
//            fail("To activate the test, uncomment the previous line and comment this line")
        }
    }

    test("FizzBuzz") {
        assertResult("FizzBuzz") {
            fizzBuzz(15)
        }
    }

    test("FizzBuzz number") {
        assertResult("16") {
            fizzBuzz(16)
        }
    }

    test("lambda") {
        assertResult(true, "lambda function definition is incorrect") {
            isEven(4)
//            fail("first provide the lambda definition, then remove this line and uncomment the above line")
        }
    }

    test("lambda is even false") {
        assertResult(false, "lambda function definition is incorrect") {
            isEven(5)
//            fail("first provide the lambda definition, then remove this line and uncomment the above line")
        }
    }

    test("lambda is odd") {
        assertResult(true, "lambda function definition is incorrect") {
            isOdd(5)
//            fail("first provide the lambda definition, then remove this line and uncomment the above line")
        }
    }

    test("lambda is odd false") {
        assertResult(false, "lambda function definition is incorrect") {
            isOdd(4)
//            fail("first provide the lambda definition, then remove this line and uncomment the above line")
        }
    }

    test("lambda double") {
        assertResult(8, "lambda function definition is incorrect") {
            timesTwo(4)
//            fail("first provide the lambda definition, then remove this line and uncomment the above line")
        }
    }

    test("HOFS function signature") {
        assertResult(List(List(2,4), List(1,3), List(20,22,24,26,28,30)), "one or both of the function types for myHOF1/2 is incorrect") {
            hofs()
        }
    }
}
