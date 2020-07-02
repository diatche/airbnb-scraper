# airbnb_scraper

## Installation

Install MongoDB and run server locally.

Clone, then run:

```sh
python3 -m venv env
source ./env/bin/activate
./setup.sh
```

## Usage

Run the spider with:

```sh
scrapy crawl airbnb \
    -a city="{cityname}" \
    -a currency="{code}" \
    -a price_min="{price_min}" \
    -a price_max="{price_max}" \
    -a instant_book={boolean} \
    -a room_type="{room_type_code}"
```

All options are optional except `city`.

Most Airbnb URL query options are also supported.

Example:

```sh
scrapy crawl airbnb \
    -a city="Auckland-CBD--Auckland--New-Zealand" \
    -a currency=NZD \
    -a instant_book=true \
    -a room_type="Entire home/apt"
```

## Acknowledgements

Original source written by [kailu3/airbnb-scraper](https://github.com/kailu3/airbnb-scraper).
