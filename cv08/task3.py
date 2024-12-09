# python task3.py --input /path/to/monitoring/dir

import argparse
import logging
import sys
import re

from pyflink.common import WatermarkStrategy, Types, Duration
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import (FileSource, StreamFormat)


def count_words_streaming(input_path):
    # init the Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)

    # define data source with file monitoring
    ds = env.from_source(
        source=FileSource.for_record_stream_format(
            StreamFormat.text_line_format(),
            input_path
        ).monitor_continuously(Duration.of_seconds(1)).build(),  # monitor directory for new files every second
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),  # use monotonous timestamps as a default
        source_name="file_source"
    )

    # fun to process lines and extract words by fisrt letter
    def extract_words(line):
        line = line.lower()
        words = re.findall(r'\b[a-záčďéěíňóřšťúůýž][a-záčďéěíňóřšťúůýž]*', line, re.UNICODE)
        for word in words:
            yield word[0], 1  # emit the first letter and count

    ds = (
        ds.flat_map(extract_words, output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
          .key_by(lambda x: x[0])
          .reduce(lambda x, y: (x[0], x[1] + y[1]))
    )

    # print the results to the console
    ds.add_sink(lambda result: print(f"{result[0]}: {result[1]}"))
    # ds.print()

    # execute the Flink job
    env.execute("Count Words (Streaming)")


if __name__ == '__main__':
    # logging
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input directory to monitor for text files.'
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    # run the program
    count_words_streaming(known_args.input)
