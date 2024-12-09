import argparse
import logging
import sys
import re
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode


def batch_word_count(input_path):
    # init Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(1)  # Single parallelism for simplicity

    # read data from files
    ds = env.read_text_file(input_path)

    # fun to extract words starting with letters A-Z
    def extract_words(line):
        line = line.lower()
        words = re.findall(r'\b[a-záčďéěíňóřšťúůýž][a-záčďéěíňóřšťúůýž]*', line, re.UNICODE)
        for word in words:
            yield word[0], 1  # emit the starting letter and count

    # process data
    ds = (
        ds.flat_map(extract_words, output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
          .key_by(lambda x: x[0])  # group by the first letter
          .reduce(lambda x, y: (x[0], x[1] + y[1]))  # count
    )

    ds.print()

    # execute the Flink batch job
    env.execute("Batch Word Count")


if __name__ == '__main__':
    # logging
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input directory containing text files.'
    )
    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    # execute program
    batch_word_count(known_args.input)
