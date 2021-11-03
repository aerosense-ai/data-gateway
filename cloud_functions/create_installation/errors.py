import json
import logging


logger = logging.getLogger(__name__)


def clean_errors(errors):
    """Reduces lists of form field errors to single items for frontend use

    This is put here because all frontend code in existence for form
    submission / error handling is already a total clusterfuck... that's
    not going to be helped by a set of useMemo(()=>{Object.keys(killMeNow).map...})

    :param dict errors: Containing a list of errors for each form field that doesn't pass validation
    :return dict: Contains a single error message for each form field that doesnt pass validation
    :rtype:
    """
    # In practicality, only the first validation error is ever shown on the front end
    logger.info(json.dumps(errors))
    for field, errorMessages in errors.items():
        errors[field] = errorMessages[0] if len(errorMessages) > 0 else "Unknown field error"

    return errors
