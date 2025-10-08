import random
import string

# Create an RDD for text file
text_file_rdd = sc.textFile('/FileStore/tables/pg72729.txt')
# Remove full stops from each line
cleaned_text_rdd = text_file_rdd.map(lambda line: line.replace('.', ''))

# Flatten the cleaned lines into words
words_file_rdd = cleaned_text_rdd.filter(lambda line: len(line) > 0).flatMap(lambda line: line.split(" "))

# Shift the RDD by one position
shifted_rdd = words_file_rdd.zipWithIndex().map(lambda x: (x[1], x[0])).sortByKey().map(lambda x: x[1]).zipWithIndex().map(lambda x: (x[1] - 1, x[0])).filter(lambda x: x[0] >= 0)

# Zip the original RDD with the shifted RDD
zipped_rdd = words_file_rdd.zipWithIndex().map(lambda x: (x[1], x[0])).join(shifted_rdd)

# Map over the zipped RDD to pair each word with its follower
paired_rdd = zipped_rdd.map(lambda x: (x[1][0], x[1][1]))

# Group by key (number) and collect the followers
grouped_followers = paired_rdd.groupByKey().mapValues(list).collect()

# Create a dictionary for efficient access to followers
term_followers_dict = {term: followers for term, followers in grouped_followers if term.strip(string.punctuation).lower() and term.strip(string.punctuation).upper() not in set(string.punctuation)}

# Create a dictionary for counts of followers for each number
term_counts = {term: {follower: followers.count(follower) for follower in followers} for term, followers in term_followers_dict.items()}





end_of_sentence_exceptions = [
    'and', 'but', 'or', 'nor', 'for', 'so', 'yet', 'however', 'although',
    'because', 'since', 'unless', 'whereas',  'if',
     'when', 'while', 'until', 'unless', 'although',
      'as',   
     'whether', 
    'whom', 'whose', 'a','an'
]


def generate_paragraph(starting_word, num_sentences, max_words_per_sentence):
    # List to store the generated sentences
    generated_text = []

    # Loop to generate the specified number of sentences
    for _ in range(num_sentences):
        # Initialize the starting term for the Markov chain for each sentence
        current_term = random.choice(words_file_rdd.collect())
        
        # List to store the current sentence, initialized with the starting term
        sentence = [current_term]
        
        # Counter for the number of words in the current sentence
        words_in_sentence = 1

        # Generate a random length for the sentence between 3 and the specified max_words_per_sentence
        sentence_length = random.randint(3, max_words_per_sentence)

        # Loop to generate the words for the current sentence
        for _ in range(sentence_length - 1):
            # Get the followers for the current term from the Markov chain dictionary
            followers = term_followers_dict.get(current_term, [])
            
            # Check if there are followers
            if followers:
                # Calculate weights based on the counts of followers for the current term
                weights = [term_counts[current_term][follower] for follower in followers]
                
                # Use random.choices to select the next term based on the weights
                next_term = random.choices(followers, weights)[0]
                
                # Append the next term to the sentence
                sentence.append(next_term)
                
                # Update the current term
                current_term = next_term
                
                # Increment the word counter
                words_in_sentence += 1

                # Check if the desired sentence length is reached
                if words_in_sentence >= sentence_length:
                    break
            else:
                break
        if sentence:
            sentence[0] = sentence[0].capitalize()


        if sentence_length > 1 and sentence[-1].lower() in end_of_sentence_exceptions:
             replacement_word = random.choice([word for word in words_file_rdd.collect() if word.lower() not in end_of_sentence_exceptions])
             sentence[-1] = replacement_word.rstrip(",.!?;:'\"") + "."
        elif sentence_length > 1:
             sentence[-1] = sentence[-1].rstrip(",.!?;:'\"") + "."
        
            

        
        
        # Join the words in the sentence and append it to the generated_text list
        generated_text.append(" ".join(sentence))

    # Join the generated sentences with spaces to form the final paragraph
    return " ".join(generated_text)

# Select random first word  
initial_word = random.choice(words_file_rdd.collect()) 




# Generate a paragraph based on 1-st order Markov chain 
paragraph_1st_order = generate_paragraph(initial_word, num_sentences=8, max_words_per_sentence=12) 
print("Generated Paragraph (1st Order):\n", paragraph_1st_order) 
