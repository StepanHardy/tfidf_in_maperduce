from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import sys
import math



WORD_RE = re.compile(r"\b[^\d\W]+\b")




class MRMostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(mapper=self.mapper_files_word,
                   reducer=self.reducer_files_word),
            MRStep(mapper=self.mapper_term_frequency,
                   reducer=self.reducer_term_frequency),
            MRStep(reducer=self.reducer_statistics)
            ]


# PHASE 1: word counting in all files

    def mapper_get_words(self, _, line):
        filepath = os.environ['map_input_file']
        file = os.path.split(filepath)[-1]
        for word in WORD_RE.findall(line):
            yield ((word.lower(), file), 1)

    def reducer_count_words(self, word_file, counts):
            yield (word_file, sum(counts))

# PHASE 2: files with a word

    def mapper_files_word(self, word_file, n):
        yield (word_file[0], (word_file[1], n, 1))

    def reducer_files_word(self, word, file_n_N):
        words = list(file_n_N)
        files_with_word = len(words)
        for file, n, _ in words:
            yield ((word, file), (n, files_with_word))

# PHASE 3: Term frequency

    def mapper_term_frequency(self, word_file, n__files_with_word):
        yield ((word_file[1]), (word_file[0], n__files_with_word[0], n__files_with_word[1]))

    def reducer_term_frequency(self, file, word_n_files_with_word):
        words = list(word_n_files_with_word)
        total_words_per_file = len(words)
        for word, n, files_with_word in words:
            if word in findings:
                tf = float(n) / total_words_per_file
                idf = math.log(float(len(file_list))) / files_with_word
                yield (file, (word, tf*idf))

# PHASE 4

    def reducer_statistics(self, file, word_tfidf):
        tfidfs = []
        for _, tfidf in word_tfidf:
            tfidfs.append(float(tfidf))
        yield 'Mean {} = {}'.format(file, float(sum(tfidfs)) / float(len(tfidfs))), ''





if __name__ == '__main__':
    file_list = os.listdir(sys.argv[1])
    findings = ['did', 'make', 'the']
    MRMostUsedWord.run()
