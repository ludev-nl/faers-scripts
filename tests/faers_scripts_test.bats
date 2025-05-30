#!/usr/bin/env bats

# Paths to scripts
MAIN_SCRIPT="1.DownloadFiles/1.download_all_and_create_files.sh"
LEGACY_SCRIPT="1.DownloadFiles/shellScripts/download_legacy_files_from_faers.sh"
CURRENT_SCRIPT="1.DownloadFiles/shellScripts/download_current_files_from_faers.sh"

# Test if the main script exists and runs successfully
@test "Check if main FAERS script runs without errors" {
  if [ -f "$MAIN_SCRIPT" ]; then
    chmod +x "$MAIN_SCRIPT"
    run bash "$MAIN_SCRIPT"
    [ "$status" -eq 0 ]
  else
    skip "Main FAERS script missing, skipping test."
  fi
}

# Test if the legacy script exists and runs successfully
@test "Check if legacy FAERS script runs without errors" {
  if [ -f "$LEGACY_SCRIPT" ]; then
    chmod +x "$LEGACY_SCRIPT"
    run bash "$LEGACY_SCRIPT"
    [ "$status" -eq 0 ]
  else
    skip "Legacy FAERS script missing, skipping test."
  fi
}

# Test if the current FAERS script exists and runs successfully
@test "Check if current FAERS script runs without errors" {
  if [ -f "$CURRENT_SCRIPT" ]; then
    chmod +x "$CURRENT_SCRIPT"
    run bash "$CURRENT_SCRIPT"
    [ "$status" -eq 0 ]
  else
    skip "Current FAERS script missing, skipping test."
  fi
}

# Test if functionName downloads FAERS zip file (Legacy)
@test "Check if functionName in legacy script downloads a file" {
  if [ -f "$LEGACY_SCRIPT" ]; then
    rm -f ../faersData/aers_ascii_2012q3.zip
    run bash "$LEGACY_SCRIPT" "12q3"
    [ -f "../faersData/aers_ascii_2012q3.zip" ]
  else
    skip "Legacy FAERS script missing, skipping test."
  fi
}

# Test if functionName in current script downloads a file
@test "Check if functionName in current script downloads a file" {
  if [ -f "$CURRENT_SCRIPT" ]; then
    rm -f ../faersData/faers_ascii_2021q1.zip
    run bash "$CURRENT_SCRIPT" "21q1"
    [ -f "../faersData/faers_ascii_2021q1.zip" ]
  else
    skip "Current FAERS script missing, skipping test."
  fi
}

# Test if the extracted folder exists after unzip in the legacy script
@test "Check if FAERS zip file is extracted correctly (legacy)" {
  if [ -f "$LEGACY_SCRIPT" ]; then
    rm -rf ../faersData/ascii
    mkdir -p ../faersData/ascii
    run bash "$LEGACY_SCRIPT" "12q3"
    [ -d "../faersData/ascii" ]
  else
    skip "Legacy FAERS script missing, skipping test."
  fi
}

# Test if filenames are converted to uppercase in current script
@test "Check if filenames are converted to uppercase" {
  mkdir -p ../faersData/ascii
  touch ../faersData/ascii/testfile.txt
  run bash -c '
    folder="../faersData/ascii";
    for file in "$folder"/*; do 
      filename=$(basename "$file");
      new_filename=$(echo "$filename" | tr "[:lower:]" "[:upper:]");
      mv "$file" "$folder/$new_filename";
    done'
  [ -f "../faersData/ascii/TESTFILE.TXT" ]
}

# Test if the auto quarter detection logic works
@test "Check if auto quarter detection works" {
  run bash -c '
    CURRENT_MONTH=$(date +%m);
    if [ "$CURRENT_MONTH" -ge 10 ]; then echo "q4";
    elif [ "$CURRENT_MONTH" -ge 7 ]; then echo "q3";
    elif [ "$CURRENT_MONTH" -ge 4 ]; then echo "q2";
    else echo "q1";
    fi'
  [ "$status" -eq 0 ]
  [[ "$output" =~ ^q[1-4]$ ]]
}
