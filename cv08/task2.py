# python task2.py --input /path/to/dir --output /path/to/output/file

import argparse
import logging
import sys
import re
import os

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import (FileSource, StreamFormat, FileSink, OutputFileConfig,
                                           RollingPolicy)


def count_czech_words_by_letter(input_path, output_path):
    # init the Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(1)  # single parallelism

    # define data source
    ds = env.from_source(
        source=FileSource.for_record_stream_format(StreamFormat.text_line_format(), input_path)
                            .process_static_file_set().build(),
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )

    # fun to process lines and extract words by first letter
    def extract_words(line):
        line = line.lower()
        words = re.findall(r'\b[a-záčďéěíňóřšťúůýž][a-záčďéěíňóřšťúůýž]*', line, re.UNICODE)
        for word in words:
            yield word[0], 1  # emit the first letter and count

    # count words by first letter
    ds = (
        ds.flat_map(extract_words, output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
          .key_by(lambda x: x[0]) 
          .reduce(lambda x, y: (x[0], x[1] + y[1]))
    )

    # define the output sink (write results to a file)
    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))

    file_sink = FileSink.for_row_format(
        base_path=output_path,
        encoder=lambda result: f"{result[0]}: {result[1]}\n".encode("utf-8")
    ).with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("output")  # prefix for the result file
        .with_part_suffix(".txt")   # extension
        .build()
    ).with_rolling_policy(
        RollingPolicy.default_rolling_policy()
    ).build()

    ds.sink_to(file_sink)

    # execute the Flink job
    env.execute("Count Czech Words")


if __name__ == '__main__':
    # logging
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=True,
        help='Input directory containing text files to process.'
    )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output directory to save the results.'
    )

    argv = sys.argv[1:]
    known_args, _ = parser.parse_known_args(argv)

    # run the program
    count_czech_words_by_letter(known_args.input, known_args.output)
