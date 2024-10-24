#!/usr/bin/env bash
# The Unix assignment is almost over: time to create a submission.
# You could create a zip folder by hand. Just place the `.sh` files in there... But where's the fun in that?
# Let's create a script that does this for us.
# This script should take an output name as the first parameter.
# If called in a directory, it should recursively find all the `.sh` files and add them to a zip folder.
# The zip folder should only contain `.sh` files and no folders.

# Check if the user has provided a name.
if [ -z "$1" ]; then
    echo "Please provide a name for the zip file."
    exit 1
fi

# Check if the name is a valid name.
if [[ "$1" != *".zip" ]]; then
    echo "Please provide a valid name for the zip file."
    exit 1
fi

# Find all the `.sh` files in the current directory and add them to the zip folder.
find . -type f -name "*.sh" -exec zip -j "$1" {} +

# Exit without an error.
exit 0