This is a simple text generator/chatbot program which generates text based on input. It was created with pyspark on the databricks platform. The generator uses 
MapReduce  model for distributed data processing where lines are flattened into words and the words are mapped in the following format (word, 1). In the reduce step
counts for each word are summed up. The probabilities for each consecutive words are based on the first order Markov chain. The python file is the full pyspark code and the html file is the original file imported from databricks.
