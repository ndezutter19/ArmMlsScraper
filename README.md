# ArmMlsScraper

## Setup

### Code Scraper
1. Download relevant environment
2. Move code scraper service and timer files from the setup folder to /etc/systemd/system/...
3. Run lines in shell to start service on timer (THIS IS UNTESTED):
~~~
sudo systemctl daemon-reload
sudo systemctl enable myservice.timer
sudo systemctl start myservice.timer
~~~