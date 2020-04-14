#!/bin/bash

echo “starting ChiSquare calculation”

cd CategoryCount
hadoop com.sun.tools.javac.Main CategoryCount.java
jar CategoryCount.jar CategoryCount*.class
hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/CategoryCounts
hadoop jar CategoryCount.jar CategoryCount /user/pknees/amazon-reviews/full/reviewscombined.json /user/e1267533/exercise_1/CategoryCounts
hadoop fs -getmerge /user/e1267533/exercise_1/CategoryCounts/ CategoryCounts.txt
hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/CategoryCounts.txt
hadoop fs -put CategoryCounts.txt /user/e1267533/exercise_1/CategoryCounts.txt

cd ../WCountPerCategory
hadoop com.sun.tools.javac.Main WCountPerCategory.java
jar cf  WCountPerCategory.jar WCountPerCategory*.class
hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/WCountPerCategory
hadoop jar WCountPerCategory.jar WCountPerCategory /user/pknees/amazon-reviews/full/reviewscombined.json /user/e1267533/exercise_1/WCountPerCategory /user/e1267533/exercise_1/stopwords.txt


cd ../ChiSquarePrepro
hadoop com.sun.tools.javac.Main ChiSquarePrepro.java
jar cf ChiSquarePrepro.jar ChiSquarePrepro*.class
hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/ChiSquarePrepro
hadoop jar ChiSquarePrepro.jar ChiSquarePrepro /user/e1267533/exercise_1/WCountPerCategory /user/e1267533/exercise_1/ChiSquarePrepro

cd ../ChiSquare
hadoop com.sun.tools.javac.Main ChiSquare.java
jar cf ChiSquare.jar ChiSquare*.class
hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/ChiSquare
hadoop jar ChiSquare.jar ChiSquare /user/e1267533/exercise_1/ChiSquarePrepro /user/e1267533/exercise_1/ChiSquare /user/e1267533/exercise_1/CategoryCounts.txt


cd ../SortingCategory
hadoop com.sun.tools.javac.Main SortingCategory.java
jar cf SortingCategory.jar SortingCategory*.class
hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/SortingCategory
hadoop jar SortingCategory.jar SortingCategory /user/e1267533/exercise_1/ChiSquare /user/e1267533/exercise_1/SortingCategory /user/e1267533/exercise_1/CategoryCounts.txt


hadoop fs -ls hdfs:///user/e1267533/exercise_1/SortingCategory