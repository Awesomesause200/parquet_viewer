## Parquet Viewer

**This is more of a personal project**

Right now, I deal with parquet and CSV files a lot! 
Turns out parquet files are amazing for AWS S3 tables, but horrible for the actual viewing experience.

This project is my attempt at fixing this issue but building a lightweight web interface. This is primarily meant to run
locally as opposed to any actual web-interface. With this, there is currently no plans to include authentication.

_____

## Features

 - **Web UI for Parquet & CSV**
   - A flask-based web app to easily view, modify, save file attributes in the browser
 - **Dynamic dtype management**
   - Track and modify column dtypes
     - Warns the user if a column would create too many nulls
     - For example, changing a string column to `int` may show a warning if >5% values are lost.
     - Detects outright failures (e.g., converting numerical data to `boolean`).
 - **Data exploration**
   - Filter and sort tables.
   - Double-click on the column header to show unique values in a popup view.
     - This also includes a search bar to search for unique values
 - **Settings Management**
   - Adjust settings with checkbox and range (potentially more later)
   - Session-based settings with persistence via configuration file (with a configuration manager)
 - **Publish results**
   - Download datasets with available file formats (CSV or Parquet)
   - Upload directly to S3 (pending testing)

_____

## Running this script

### Prerequisites
 - Python 3.10+
 - Install requirements
   - `pip install -r requirements.txt`

### Running the application
 - Ensure you have a .env file containing the key `flask_secret_key`
 - You can run it normally or in debug mode
   - flask run
   - `python3 app.py`
