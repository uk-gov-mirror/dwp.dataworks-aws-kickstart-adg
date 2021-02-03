#!/usr/bin/env bash
work_dir=`echo $PWD`
if [ -d "$work_dir/temp" ]; then
  rm -rf "$work_dir/temp"
fi

mkdir "$work_dir/temp"
cp "$work_dir/steps/spark/main.py" "$work_dir/temp"
cd "$work_dir/steps/spark" && zip -x main.py -r "$work_dir/temp/jobs.zip" .
