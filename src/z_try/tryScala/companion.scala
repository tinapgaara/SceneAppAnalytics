package z_try.tryScala

/**
 * Created by max2 on 6/24/15.
 */


object companion{
  var m_scalamember = 1
  def hello(): Unit ={
    println("hello object companion")
  }

  def testFunc(int_1 : Int, int_2 : Int): Int = {
    return int_1 + int_2
  }
}
