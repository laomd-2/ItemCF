javac ItemCF.java
jar cf itemcf.jar *.class
rm *.class

if [ -f "itemcf.jar" ];then
	hadoop jar itemcf.jar ItemCF /input /itemcf-output
	rm output -r
	mkdir output
	echo pulling user score matrix ...
	hdfs dfs -get /temp1/part-r-00000 output/user-score-matrix.txt
	echo pulling occurrence matrix ...
	hdfs dfs -get /temp2/part-r-00000 output/item-occurrence-matrix.txt
	echo pulling result ...
	hdfs dfs -get /itemcf-output/part-r-00000 output/result.txt
	hdfs dfs -rm -r /temp1 /temp2 /itemcf-output
	rm itemcf.jar
fi
