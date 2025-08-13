## Parquet Viewer

**This is more of a personal project**

Right now, I deal with parquet and CSV files a lot! 
Turns out parquet files are amazing for AWS S3 tables, but horrible for the actual viewing experience.

_____

In comes this project! The current scope of the project is small and focused, but there might be feature creep in the future.
 - Create a flask app with an interface to help build a web UI to view parquet and CSV files
 - Create a mechanism to track on what datatypes are assigned to what columns and allow the user to change it
   - An important part of this task is to help the user avoid causing issues by informing them when >5% of the column's values would be changed to null with a given dtype conversion, or if it will outright fail
   - Outright failing would entail changing a column with numerical values to a boolean field.
 - Allow the user to filter, sort, and do light manipulation on the data by allowing unique views
   - One such unique view is double-clicking on the column header allows for a smaller view to appear with unique values

Not all features are complete yet, such as the S3 integration. That is intended for a future release.