from flask_wtf import FlaskForm
from wtforms import StringField, validators


class CreateInstallationForm(FlaskForm):
    reference = StringField("Reference", [validators.DataRequired()])
    hardware_version = StringField("Hardware version", [validators.DataRequired()])
    longitude = StringField("Longitude", [validators.DataRequired()])
    latitude = StringField("Latitude", [validators.DataRequired()])
