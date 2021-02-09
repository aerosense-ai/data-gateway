import logging
import pkg_resources


logger = logging.getLogger(__name__)


class Uploader:
    """Handler class for HTTPS-based uploads of events and audio files"""

    def __init__(self, configuration=None, **kwargs):
        """Instantiate and configure gateway server"""
        pass

    def _validate_event(self):
        """Validate event data against the required schema"""
        file_name = pkg_resources.resource_string("gateway", "schema/event_schema.json")
        logger.info("file name is %s", file_name)

    def _validate_audio(self):
        """Validate audio+meta data against the required schema"""
        file_name = pkg_resources.resource_string("gateway", "schema/audio_meta_schema.json")
        logger.info("file name is %s", file_name)
