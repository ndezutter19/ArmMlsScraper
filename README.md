# ArmMlsScraper

## Setup

### Code Scraper
The Phoenix code scraper runs through the code enforcement website form and scrapes addresses with open violations, entry point is at PhoenixScraper.py which uses PhoenixCodesv2.py and MaricopaParcel.py as scripts to collect all of the information and adds them to a file. Once the script has finished running it pipes the file to AirtableHelper.py which adds all of the entries to the AirTable database.

1. Download relevant environment and move to ./mlsVenv.
2. Move code scraper service and timer files from the setup folder to /etc/systemd/system/...
3. Run lines in shell to start service on timer (THIS IS UNTESTED):
~~~
sudo systemctl daemon-reload
sudo systemctl enable codeScraper.timer
sudo systemctl start codeScraper.timer
~~~
