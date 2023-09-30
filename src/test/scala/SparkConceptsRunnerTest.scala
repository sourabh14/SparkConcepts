import org.scalatest.funsuite.AnyFunSuite

class SparkConceptsRunnerTest extends AnyFunSuite {

    test("StringUtils.convertToUpperCase") {
        val testString = "TestString"
        assertResult("TESTSTRING") {
            StringUtils.convertToUpperCase(testString)
        }
    }

}
