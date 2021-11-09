from flask_wtf import FlaskForm
from wtforms import FloatField, StringField, validators


class CreateInstallationForm(FlaskForm):
    reference = StringField("Reference", [validators.DataRequired()])
    hardware_version = StringField("Hardware version", [validators.DataRequired()])
    longitude = FloatField("Longitude", [validators.Optional()])
    latitude = FloatField("Latitude", [validators.Optional()])
