# ArmMlsScraper

## Setup

### Code Scraper
1. Download relevant environment and move to ./mlsVenv.
2. Move code scraper service and timer files from the setup folder to /etc/systemd/system/...
3. Run lines in shell to start service on timer (THIS IS UNTESTED):
~~~
sudo systemctl daemon-reload
sudo systemctl enable codeScraperTimer.timer
sudo systemctl start codeScraperTimer.timer
~~~