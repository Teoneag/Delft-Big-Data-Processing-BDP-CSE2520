#!/usr/bin/env bash
# This program should take a fileIn as the first parameter:
# It takes the input log file that has the same format as the `access_log` file and maps it to a CSV format.
# The CSV format is:
# Client,Time,Type,Path,Status,Size
#
# The program should not create a CSV file.
# This can be done by piping the output to a file.
# Example: `./logToCSV access_log > output.csv`
# It could take some time to convert all of the `access_log` file contents. Consider using a small subset for testing.

# Check if the user has provided a file.
if [ -z "$1" ]; then
    echo "Please provide a file to convert."
    exit 1
fi

# Check if the file exists.
if [ ! -f "$1" ]; then
    echo "The file does not exist."
    exit 1
fi

# Read the file and convert it to CSV.
# awk '{print $1","$4","$6","$7","$9","$10}' "$1" | tr -d '"'
# awk '{split($7, path, "?"); print $1","$4","$6","path[1]","$9","$10}' "$1" | tr -d '[' | tr -d '"'
# awk '{split($7, path, "?"); print $1","$4" "$5","$6","path[1]","$9","$10}' "$1" | tr -d '[]' | tr -d '"'
# awk '{print $1","$4" "$5","$6","$7","$9","$10}' "$1" | tr -d '[]' | tr -d '"'
# awk '{print $1","$4" "$5","$6","substr($7, 1, length($7)-9)","$9","$10}' "$1" | tr -d '[]"' 
awk '{print $1","substr($4,2)","$6","$7","$9","$10}' "$1" | tr -d '"'

# Exit without an error.
exit 0