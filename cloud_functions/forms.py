from flask_wtf import FlaskForm
from slugify import slugify
from wtforms import FloatField, StringField, validators
from wtforms.validators import StopValidation


class SlugifiedValidator:
    def __call__(self, form, field):
        """Validate that the value of the given form field is slugified.

        :param flask_wtf.form.FlaskForm form: the form the field belongs to
        :param wtforms.fields.core.Field field: the field to validate
        :raise wtforms.validators.StopValidation: if the field is not slugified
        :return None:
        """
        if field.raw_data[0] != slugify(field.raw_data[0]):
            raise StopValidation("This field must be slugified.")


class CreateInstallationForm(FlaskForm):
    reference = StringField("Reference", [validators.DataRequired(), SlugifiedValidator()])
    turbine_id = StringField("Turbine ID", [validators.DataRequired()])
    receiver_firmware_version = StringField("Receiver firmware version", [validators.DataRequired()])
    longitude = FloatField("Longitude", [validators.Optional()])
    latitude = FloatField("Latitude", [validators.Optional()])


class AddSensorTypeForm(FlaskForm):
    name = StringField("Name", [validators.DataRequired()])
    reference = StringField("Reference", [validators.DataRequired(), SlugifiedValidator()])
    description = StringField("Description", [validators.Optional()])
    measuring_unit = StringField("Measuring unit", [validators.Optional()])
    metadata = StringField("Metadata", [validators.Optional()])
