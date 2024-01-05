# Python-Spark-Log-Analysis

Python scripts to process, and analyze log files using PySpark.

> 👋 This repository's maintainer is available to hire for Python/PySpark consulting projects. To get a cost estimate, send email to lallyelias87@gmail.com (for projects of any size or complexity).

## Requirements

- [Python 3.8+](https://www.python.org/)
- [pip 23.3+](https://github.com/pypa/pip)
- [pyarrow 14.0+](https://github.com/apache/arrow)
- [pandas 2.0+](https://github.com/pandas-dev/pandas)
- [pyspark 3.5+](https://github.com/apache/spark/tree/master/python)
- [spark-nlp 5.2.2+](https://github.com/JohnSnowLabs/spark-nlp)

## Usage

- Clone this repository
```sh
git clone https://github.com/lykmapipo/Python-Spark-Log-Analysis.git
cd Python-Spark-Log-Analysis
```

- Install all dependencies

```sh
pip install -r requirements.txt
```

- To `parse and prepare` raw log files, run:
```sh
python prepare.py
```

- To get `basic summary report` of structured logs, run:
```sh
python summarize.py
```

- To perform `word frequency analysis` on structured logs, run:
```sh
python count_words.py
```

## Data
- Check [data/raw](https://github.com/lykmapipo/Python-Spark-Log-Analysis/tree/main/data/raw) directory for all `raw logs` data. Each file is in `text format`, and each log entry is format as `[timestamp] level: message`.

- Check [data/interim/structured-logs](https://github.com/lykmapipo/Python-Spark-Log-Analysis/tree/main/data/interim/structured-logs) directory for `structured logs` data generated when run `prepare.py`. Each file is in `parquet format`, and each log entry follow below `spark` schema:
```sh
root
 |-- log_timestamp: timestamp (nullable = true)
 |-- log_level: string (nullable = true)
 |-- log_message: string (nullable = true)
 |-- log_length: integer (nullable = true)
 |-- log_year: integer (nullable = true)
 |-- log_month: integer (nullable = true)
 |-- log_day: integer (nullable = true)
 |-- log_hour: integer (nullable = true)
 |-- log_minute: integer (nullable = true)
 |-- log_second: integer (nullable = true)
 |-- log_message_length: integer (nullable = true)
```

- Check [summary report](https://github.com/lykmapipo/Python-Spark-Log-Analysis/blob/main/data/reports/summary_report.csv) generated when run `summarize.py`.

- Check [word frequency analysis report](https://github.com/lykmapipo/Python-Spark-Log-Analysis/blob/main/data/reports/word_count.csv) generated when run `count_words.py`.

## Contribute

It will be nice, if you open an issue first so that we can know what is going on, then, fork this repo and push in your ideas. Do not forget to add a bit of test(s) of what value you adding.

## Questions/Issues/Contacts

lallyelias87@gmail.com, or open a GitHub issue


## Licence

The MIT License (MIT)

Copyright (c) lykmapipo & Contributors

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
