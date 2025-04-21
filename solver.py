from Pyro4 import expose
import random


class Solver:
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers
        print("Inited")

    def solve(self):
        print("Job Started")
        print("Workers %d" % len(self.workers))

        text = self.read_input()

        # map
        mapped = []
        for i in xrange(0, len(self.workers)):
            print("map %d" % i)
            mapped.append(self.workers[i].mymap(text))

        output_dict = self.myreduce(mapped)

        self.write_output(self.format_output(output_dict))

        print("Job Finished")

    @staticmethod
    @expose
    def myreduce(mapped):
        output_dict = {}
        for word_freq in mapped:
            word_freq = word_freq.value
            for word, count in word_freq.items():
                output_dict[word] = output_dict.get(word, 0) + count
        return output_dict

    @staticmethod
    @expose
    def mymap(text):
        word_freq = {}
        words = text.lower().split()

        for word in words:
            word = word.strip('.,!?:;"\'')
            if word:
                word_freq[word] = word_freq.get(word, 0) + 1

        return word_freq

    def read_input(self):
        f = open(self.input_file_name, 'r')
        text = f.read()
        f.close()
        return text

    def format_output(self, output_dict):
        result = ""
        for word, count in output_dict.items():
            result += "{}:{}\n".format(word, count)
        return result

    def write_output(self, output):
        f = open(self.output_file_name, 'w')
        f.write(output)
        f.close()
        print("output done")

