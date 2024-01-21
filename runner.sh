#!/bin/bash

# Define the path to your Python script
PYTHON_SCRIPT_PATH="/opt/BNATOKEN/TOKEN.py"

# Define the exclusion dates 2024 NSE holidays
EXCLUSION_DATES=("2024-01-26" "2024-03-08" "2024-03-25" "2024-03-29" "2024-04-11" "2024-04-17" "2024-05-01" "2024-06-17" "2024-07-17" "2024-08-15" "2024-10-02" "2024-11-01" "2024-11-15" "2024-12-25")

# Get the current date
CURRENT_DATE=$(date +"%Y-%m-%d")

# Check if the current date is a weekday and not in the exclusion list
if [ $(date +"%u") -le 5 ] && [[ ! " ${EXCLUSION_DATES[@]} " =~ " ${CURRENT_DATE} " ]]; then
    # Check if the current time is 9:17 am IST
    python3 "$PYTHON_SCRIPT_PATH"
fi
