cd exercise_1/src/CategoryCount
hadoop com.sun.tools.javac.Main CategoryCount.java
jar cf CategoryCount.jar CategoryCount*.class
hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/CategoryCounts
hadoop jar CategoryCount.jar CategoryCount /user/pknees/amazon-reviews/full/reviews_devset.json /user/e1267533/exercise_1/CategoryCounts
hadoop fs -getmerge /user/e1267533/exercise_1/CategoryCounts/ CategoryCounts.txt
hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/CategoryCounts.txt
hadoop fs -put CategoryCounts.txt /user/e1267533/exercise_1/CategoryCounts.txt

cd ../WCountPerCategory
hadoop com.sun.tools.javac.Main WCountPerCategory.java
jar cf  WCountPerCategory.jar WCountPerCategory*.class
hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/WCountPerCategory
hadoop jar WCountPerCategory.jar WCountPerCategory /user/pknees/amazon-reviews/full/reviews_devset.json /user/e1267533/exercise_1/WCountPerCategory /user/e1267533/exercise_1/stopwords.txt


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

cd ../../results
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00000 | head -200 >  cat0.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00001 | head -200 >  cat1.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00002 | head -200 >  cat2.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00003 | head -200 >  cat3.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00004 | head -200 >  cat4.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00005 | head -200 >  cat5.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00006 | head -200 >  cat6.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00007 | head -200 >  cat7.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00008 | head -200 >  cat8.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00009 | head -200 >  cat9.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00010 | head -200 >  cat10.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00011 | head -200 >  cat11.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00012 | head -200 >  cat12.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00013 | head -200 >  cat13.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00014 | head -200 >  cat14.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00015 | head -200 >  cat15.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00016 | head -200 >  cat16.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00017 | head -200 >  cat17.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00018 | head -200 >  cat18.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00019 | head -200 >  cat19.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00020 | head -200 >  cat20.txt
hadoop fs -cat hdfs:///user/e1267533/exercise_1/SortingCategory/part-r-00021 | head -200 >  cat21.txt

hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/SortingCategoryShort/

hadoop fs -mkdir hdfs:///user/e1267533/exercise_1/SortingCategoryShort/

hadoop fs -put cat0.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat0.txt
hadoop fs -put cat1.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat1.txt
hadoop fs -put cat2.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat2.txt
hadoop fs -put cat3.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat3.txt
hadoop fs -put cat4.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat4.txt
hadoop fs -put cat5.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat5.txt
hadoop fs -put cat6.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat6.txt
hadoop fs -put cat7.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat7.txt
hadoop fs -put cat8.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat8.txt
hadoop fs -put cat9.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat9.txt
hadoop fs -put cat10.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat10.txt
hadoop fs -put cat11.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat11.txt
hadoop fs -put cat12.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat12.txt
hadoop fs -put cat13.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat13.txt
hadoop fs -put cat14.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat14.txt
hadoop fs -put cat15.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat15.txt
hadoop fs -put cat16.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat16.txt
hadoop fs -put cat17.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat17.txt
hadoop fs -put cat18.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat18.txt
hadoop fs -put cat19.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat19.txt
hadoop fs -put cat20.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat20.txt
hadoop fs -put cat21.txt hdfs:///user/e1267533/exercise_1/SortingCategoryShort/cat21.txt

cd ../src/finalOutput
hadoop com.sun.tools.javac.Main finalOutput.java
jar cf finalOutput.jar finalOutput*.class
hadoop fs -rm -R -skipTrash  /user/e1267533/exercise_1/finalOutput
hadoop jar finalOutput.jar finalOutput /user/e1267533/exercise_1/SortingCategoryShort /user/e1267533/exercise_1/finalOutput

cd ../../results 

hadoop fs -getmerge /user/e1267533/exercise_1/finalOutput total.txt
