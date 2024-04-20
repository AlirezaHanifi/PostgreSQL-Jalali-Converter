# Python Jalali Converter with Postgres Database

This repository provides a Python script to create a Postgres database table for converting Gregorian dates to Jalali dates. Additionally, the table includes pre-populated Jalali holiday information.

## Getting Started

1. Clone the repository:
```bash
git clone https://github.com/AlirezaHanifi/PostgreSQL-Jalali-Converter.git
```

2. Rename the `.env.example` file to `.env`.
```bash
cp .env.sample .env
```

3. Update the `.env` file with your desired Postgres connection details (host, database name, user, password).

4. Create virtual environments and install dependencies: 
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

5. Run the script:
```bash
python3 postgresql_jalali_converter.py
```
This script will create the table in your Postgres database. 

## Features

* Convert Gregorian dates to Jalali dates.
* Pre-populated table with common Jalali holidays.
* Leverages Postgres database for efficient date conversion and storage.
