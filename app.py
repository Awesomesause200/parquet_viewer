# flask_parquet_app.py
from flask import Flask, render_template, request, redirect, url_for, send_file, flash, jsonify, session
from markupsafe import Markup
import pandas as pd
import os
import io
import boto3
from werkzeug.utils import secure_filename
from configuration_manager import ConfigManager
from additional_utilites import merge_settings, read_data

app = Flask(__name__)
app.secret_key = os.environ.get("flask_secret_key")

UPLOAD_FOLDER = "uploads"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
global_warnings: list = []

# Globals to hold the current dataframe and its filename
current_df: pd.DataFrame = pd.DataFrame()
original_df: pd.DataFrame = pd.DataFrame()
current_filename: str | None = None

# Global dictionary for default settings. This is now the single source of truth.
global_configuration_manager = ConfigManager()
DEFAULT_SETTINGS = global_configuration_manager.read_configuration()


# Load from S3 or local file
def load_dataframe(file, is_s3: bool = False):
    if is_s3:
        s3 = boto3.client("s3")
        bucket, key = file.replace("s3://", "").split("/", 1)
        obj = s3.get_object(bucket, Key=key)
        data = obj["Body"].read()
        parquet_stream, csv_stream = io.BytesIO(data), io.StringIO(data.decode())
        ext = key.split("/")[-1].lower() if "." in key else None
    else:
        parquet_stream, csv_stream = file, file
        ext = file.name.split(".")[-1].lower() if "." in file.name else None

    # Attempt to read by file extension, otherwise read whichever stream works first
    if ext == "parquet":
        return read_data(pd.read_parquet, parquet_stream)
    elif ext == "csv":
        return read_data(pd.read_csv, csv_stream)
    else:
        return read_data(pd.read_parquet, parquet_stream) or read_data(pd.read_csv, csv_stream)


@app.route("/", methods=["GET", "POST"])
def home():
    global current_df, original_df, current_filename, global_warnings
    global_warnings = []

    # Initialize settings in the session if they don"t exist
    if "settings" not in session:
        session["settings"] = DEFAULT_SETTINGS.copy()  # Use .copy() to prevent modifying the global dict

    if request.method == "POST":
        if "file" in request.files:
            file = request.files["file"]
            if file.filename:
                filename = secure_filename(file.filename)
                filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
                file.save(filepath)
                with open(filepath, "rb") as f:
                    current_df = load_dataframe(f)

                try:
                    # Check if we got None returned (parser failed)
                    if not isinstance(current_df, pd.DataFrame):
                        global_warnings = [
                            f"Failed to import file {file.filename} (invalid file type) . Please try again later or choose a different file.",
                            "Acceptable formats: parquet, csv"]
                        return render_template("home.html", global_warnings=global_warnings)
                    else:
                        original_df = current_df.copy()
                except Exception as e:
                    global_warnings = [f"Failed to parse file due to uncaught exception. {str(e)}"]
                    return render_template("home.html", global_warnings=global_warnings)

                current_filename = filename
        elif "s3_uri" in request.form:
            s3_uri = request.form["s3_uri"]
            current_df = load_dataframe(s3_uri, is_s3=True)
            original_df = current_df.copy()
            current_filename = s3_uri
        return redirect(url_for("display"))
    return render_template("home.html")


@app.route("/settings", methods=["GET", "POST"])
def settings():
    global global_configuration_manager

    # Load saved settings from session (maybe empty or partial)
    saved_settings = session.get("settings", {})

    # Merge with defaults (favors saved settings from file system)
    session["settings"] = merge_settings(DEFAULT_SETTINGS, saved_settings)
    settings = session["settings"]

    if request.method == "POST":
        for key, value in request.form.items():
            if key in settings:
                setting = settings[key]
                if isinstance(setting, dict):
                    if setting.get("type") == "range":
                        setting["value"] = float(value) / 100.0
                    elif setting.get("type") == "checkbox":
                        setting["value"] = True
                    else:
                        setting["value"] = value
            else:
                # Handling string/int type keys
                settings[key] = value

        # Handle unchecked checkboxes
        for key, setting in settings.items():
            if isinstance(setting, dict) and setting.get("type", "") == "checkbox" and key not in request.form:
                setting["value"] = False

        # Update and save settings
        print(settings)
        session["settings"] = settings
        global_configuration_manager.configuration = settings
        global_configuration_manager.save_configuration()
        return redirect(url_for("settings"))

    return render_template("settings.html", settings=settings)



@app.route("/display")
def display():
    global current_df, current_filename
    if current_df is None:
        flash("No file loaded. Please upload or provide S3 URI.")
        return redirect(url_for("home"))

    # Retrieve the null ratio from the session to pass to the template
    null_ratio_alert_threshold = session.get("settings", DEFAULT_SETTINGS)["null_ratio_alert_threshold"]["value"]

    dtypes = current_df.dtypes.astype(str).to_dict()
    dtypes = {k: "string" if v == "object" else v for k, v in dtypes.items()}  # If it is an object dtype, denote string
    html_table = current_df.to_html(classes="table table-dark table-striped text-center", index=False)

    return render_template(
        "display.html",
        tables=Markup(html_table),
        dtypes=dtypes,
        filename=current_filename,
        NULL_RATIO_ALERT_THRESHOLD=null_ratio_alert_threshold  # Pass the dynamic value
    )


@app.route("/check_conversion", methods=["POST"])
def check_conversion():
    global current_df
    if current_df is None:
        return jsonify({"error": "No file loaded."}), 400

    data = request.get_json()
    column = data.get("column")
    new_type = data.get("new_dtype")

    if column not in current_df.columns:
        return jsonify({"error": "Column not found."}), 400

    warning = None

    # Get the dynamic threshold from the session
    settings = session.get("settings", DEFAULT_SETTINGS)
    null_ratio_alert_threshold = settings["null_ratio_alert_threshold"]["value"]

    try:
        if new_type == "int":
            converted = pd.to_numeric(current_df[column], errors="coerce", downcast="integer").astype("Int32")
        elif new_type == "float":
            converted = pd.to_numeric(current_df[column], errors="coerce", downcast="float").astype("float32")
        elif new_type == "bool":
            converted = current_df[column].astype("boolean")
        elif new_type == "datetime":
            converted = pd.to_datetime(current_df[column], errors="coerce")
        elif new_type == "string":
            converted = current_df[column].astype("string")
        else:
            return jsonify({"warning": None})

        # Perform the null ratio check using the dynamic value
        null_ratio = converted.isna().sum() / len(converted)
        if null_ratio > null_ratio_alert_threshold:
            warning = f"Conversion will result in {null_ratio:.2%} nulls."

    except Exception:
        warning = f"Conversion to {new_type} will fail."

    return jsonify({"warning": warning})


@app.route("/change_dtypes", methods=["GET", "POST"])
def change_dtypes():
    global current_df
    if current_df is None:
        flash("No file loaded to modify.")
        return redirect(url_for("home"))

    # Get the dynamic threshold from the session
    settings = session.get("settings", DEFAULT_SETTINGS)
    null_ratio_alert_threshold = settings["null_ratio_alert_threshold"]["value"]

    dtype_options = ["string", "int", "float", "bool", "datetime"]
    if request.method == "POST":
        changes = {}
        warnings = []

        for col in current_df.columns:
            new_type = request.form.get(col)
            if new_type and new_type != "no_change":
                try:
                    if new_type == "int":
                        converted = pd.to_numeric(current_df[col], errors="coerce", downcast="integer").astype("Int32")
                    elif new_type == "float":
                        converted = pd.to_numeric(current_df[col], errors="coerce", downcast="float").astype("float32")
                    elif new_type == "bool":
                        converted = current_df[col].astype("boolean")
                    elif new_type == "datetime":
                        converted = pd.to_datetime(current_df[col], errors="coerce")
                    elif new_type == "string":
                        converted = current_df[col].astype("string")
                    else:
                        continue

                    null_ratio = converted.isna().sum() / len(converted)
                    if null_ratio > null_ratio_alert_threshold:
                        warnings.append(
                            f"Column '{col}' conversion to {new_type} will result in {null_ratio:.2%} nulls.")

                    changes[col] = converted
                except Exception as e:
                    flash(f"Failed to convert column {col} to {new_type}: {e}")

        for col, new_col in changes.items():
            current_df[col] = new_col

        for warning in warnings:
            flash(warning)

        return redirect(url_for("display"))

    current_dtypes = current_df.dtypes.astype(str).to_dict()
    current_dtypes = {k: "string" if v == "object" else v for k, v in current_dtypes.items()}
    return render_template("change_dtypes.html", columns=current_df.columns, dtypes=current_dtypes,
                           options=dtype_options)


@app.route("/revert_dtypes")
def revert_dtypes():
    global current_df, original_df
    if original_df is not None:
        current_df = original_df.copy()
        flash("Data types reverted to original.")
    return redirect(url_for("display"))


@app.route("/publish", methods=["GET", "POST"])
def publish():
    global current_df, current_filename
    if current_df is None:
        flash("No data loaded to publish.")
        return redirect(url_for("home"))
    if request.method == "POST":
        fmt = request.form["format"]
        filename = request.form.get("filename") or "data"
        filename = os.path.splitext(filename)[0]
        final_name = f"{filename}.{fmt}"
        if "download" in request.form:
            buf = io.BytesIO()
            if fmt == "csv":
                current_df.to_csv(buf, index=False)
                buf.seek(0)
                return send_file(buf, mimetype="text/csv", as_attachment=True, download_name=final_name)
            elif fmt == "parquet":
                current_df.to_parquet(buf, index=False)
                buf.seek(0)
                return send_file(buf, mimetype="application/octet-stream", as_attachment=True,
                                 download_name=final_name)
        elif "s3_uri" in request.form:
            s3_uri = request.form["s3_uri"]
            s3 = boto3.client("s3")
            bucket, key = s3_uri.replace("s3://", "").split("/", 1)
            if not key.endswith(f".{fmt}"):
                key += f".{fmt}"
            buf = io.BytesIO()
            if fmt == "csv":
                current_df.to_csv(buf, index=False)
            elif fmt == "parquet":
                current_df.to_parquet(buf, index=False)
            buf.seek(0)
            s3.upload_fileobj(buf, bucket, key)
            flash(f"File uploaded to {s3_uri}")
    return render_template("publish.html", 
                           filename=current_filename, 
                           default_filename=os.path.splitext(current_filename or "data")[0])


if __name__ == "__main__":
    app.run(debug=True)
