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
    hardware_version = StringField("Hardware version", [validators.DataRequired()])
    longitude = FloatField("Longitude", [validators.Optional()])
    latitude = FloatField("Latitude", [validators.Optional()])
