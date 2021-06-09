import argparse
import json
import os

from pyspark import SparkConf, SparkContext

# The fields of the events in their json format
USERS = "users"
IS_MONO = "monotonic"
WEBSITE = "website"
TIMESTAMP = "timestamp"

# The indexes of each field in the PairRDD format.
TIMESTAMP_IND = 0
USERS_IND = 1
MONOTONIC_IND = 2
KEY_INDEX = 0
VALUE_INDEX = 1


def is_monotonic(first, sec):
    """
    The reduction function that calculates the monotonicity of each website.
    It checks each two events if they are monotonic to each other and updates
    the monotonic field according to the result. The format of each event is:
    [timestamp, users amount, monotonic flag]
    :param list first: The first event in the above format.
    :param list sec: The second event in the above format.
    :return: The second event with an updated monotonic field, according to
    the reduction.
    :rtype: list
    """
    is_mono = first[MONOTONIC_IND] and first[USERS_IND] <= sec[USERS_IND]
    return [sec[TIMESTAMP_IND], sec[USERS_IND], is_mono]


def append_is_mono_field(event):
    """
    Add the monotonic field to a given event.
    :param dict event: The event to add the monotonic field to. i.e:
        {"timestamp": "2018-01-02T00:00:00.000Z", "website": "sitea",
        "users": 13} =>
        {"timestamp": "2018-01-02T00:00:00.000Z", "website": "sitea",
        "users": 13, "monotonic": True}
    :return: The updated event.
    :rtype: dict
    """
    event[IS_MONO] = True
    return event


def format_result(result_event):
    """
    Formats the final results from the PairRDD format to their final format.
    :param result_event: A result event in the PairRDD format.
    :return: The result in the final format, i.e:
        {"website": "a", "monotonic": true}
    :rtype: dict
    """
    return {
        WEBSITE: result_event[KEY_INDEX],
        IS_MONO: result_event[VALUE_INDEX][MONOTONIC_IND]
    }


def parse_path_args():
    """
    Parses the arguments to the program. There should be two arguments - the
    path to the input dir, and the path to the output dir.
    :return: The arguments paths: input path, and output path.
    :rtype: tuple
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("input_path", help="The path to the input directory")
    parser.add_argument("output_path", help="The path to the output directory")
    args = parser.parse_args()
    return args.input_path, args.output_path


def main():
    input_path, output_path = parse_path_args()
    input_files_pattern = os.path.join(input_path, "*")

    conf = SparkConf().setAppName("Monotonic Function").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # Get all the events from all input files and sort them by timestamp.
    events = sc.textFile(f"{input_files_pattern}").map(json.loads).map(
        append_is_mono_field).sortBy(lambda event: event[TIMESTAMP])

    # Convert the RDD to PairRDD of the next format -
    # (website_id, [timestamp, users amount, monotonic flag])
    pair_events = events.map(
        lambda event: (event[WEBSITE],
                       [event[TIMESTAMP], event[USERS], event[IS_MONO]]))

    # Using reduction by key in order to calculate the monotonicity of each
    # website.
    results = pair_events.reduceByKey(is_monotonic).map(format_result)
    results.saveAsTextFile(output_path)


if __name__ == '__main__':
    main()
