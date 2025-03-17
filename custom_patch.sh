#!/bin/bash

# Define the original file and the new file
original_file="postgresql-13.1/src/backend/optimizer/path/costsize.c"
new_file="scripts/original/costsize.c"
patch_file="scripts/patch/costsize.patch"

# Check if the argument is 'create' or 'apply'
if [ "$1" == "create" ]; then
    # Create the patch
    diff -Naru -w "$original_file" "$new_file" > "$patch_file"
    echo "Patch created at $patch_file"
elif [ "$1" == "apply" ]; then
    # Apply the patch to the original file
    patch -p0 < "$patch_file"
    echo "Patch applied from $patch_file"
else
    echo "Invalid argument. Use 'create' to create a patch or 'apply' to apply a patch."
    exit 1
fi
