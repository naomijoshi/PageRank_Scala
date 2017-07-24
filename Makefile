# Add input files to the input folder

# Compiles code and run jar.
run:
	rm -rf output
	(cd PageRankScala && sbt "run ../input ../output")

	
# Removes local output directory and jars 
clean:
	rm -rf output
	(cd PageRankScala && sbt clean)
	
	

