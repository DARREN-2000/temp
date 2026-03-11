# Marketing Automation Data Pipeline

I built this pipeline to automate the boring parts of marketing analytics. It pulls in campaign data, cleans it up, scores leads based on how engaged they are, and spits out reports the marketing team can actually use. The whole thing is a portfolio project but it solves a real problem that most marketing teams deal with. Bad data and no easy way to figure out which leads are worth chasing.

## What it does

You feed it campaign data from CSV and JSON files (the kind you export from email platforms, web analytics tools, CRM systems). It runs through a cleaning and validation step, scores each lead from 0 to 100 based on their engagement, groups them into Hot, Warm, or Cold buckets, and then calculates ROI numbers for each campaign. Everything gets saved to reports you can hand off to the team.

## How the data flows

Raw data comes in from the `data/` folder. The pipeline validates emails, removes duplicates, fills in missing fields, then passes everything through the scoring engine. Final output goes to `reports/` as CSV and JSON files plus a SQLite database if you want to query things later.

## What it can do

- Reads campaign data from CSV and JSON files
- Checks email formats and throws out invalid ones
- Removes duplicate records so you don't count the same person twice
- Scores leads on a 0 to 100 scale using open rates, click rates, page visits, and content downloads
- Figures out cost per lead, conversion rates, and ROI for each campaign
- Groups leads into Hot, Warm, and Cold segments so the sales team knows who to call first
- Exports everything to CSV, JSON, and SQLite

## Built with

- Python 3.9+
- pandas for data processing
- SQLite for storing results
- Standard logging for tracking what the pipeline does

## Getting started

```bash
pip install -r requirements.txt

python generate_sample_data.py

python pipeline.py
```

After running this check the `reports/` folder for your output files.

## Project files

- `pipeline.py` is the main script that runs everything
- `generate_sample_data.py` creates fake but realistic data for testing
- `lead_scoring.py` has the scoring logic
- `requirements.txt` lists the Python packages you need
- `tests/test_pipeline.py` has the tests

## How the scoring works

Every lead gets a score between 0 and 100. The score is based on four things and each one has a different weight.

Email open rate counts for 20% of the score. Click through rate is 30% because if someone is clicking links they are actually interested. Page visits make up 25% and content downloads are the remaining 25%.

Once the score is calculated the lead gets put into a segment. Score 70 or above means Hot and these people are ready to talk to sales. Between 40 and 69 is Warm, meaning they need more nurturing before they are ready. Below 40 is Cold, they are not really engaging with the content.

## Campaign ROI

For each campaign the pipeline works out how much was spent versus how many people converted. It gives you cost per lead, the conversion rate as a percentage, and the overall ROI. This makes it easy to see which channels are actually working and which ones are burning money.

## Output files

Everything ends up in the `reports/` folder.

`scored_leads.csv` has every lead with their score and segment. `campaign_summary.json` has the ROI numbers broken down by campaign. There is also `pipeline_run.log` which logs what happened during the run including how many records were cleaned out and why.

## License

MIT
