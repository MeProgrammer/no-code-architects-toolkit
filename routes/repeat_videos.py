from flask import Blueprint
from app_utils import *
import logging
from services.ffmpeg_toolkit import process_videos_repetition
from services.authentication import authenticate

repeat_bp = Blueprint('repeat', __name__)
logger = logging.getLogger(__name__)

@repeat_bp.route('/repeat-videos', methods=['POST'])
@authenticate
@validate_payload({
    "type": "object",
    "properties": {
        "videos": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "video_url": {"type": "string", "format": "uri"},
                    "repeat_count": {"type": "integer", "minimum": 1}
                },
                "required": ["video_url", "repeat_count"]
            },
            "minItems": 1
        },
        "webhook_url": {"type": "string", "format": "uri"},
        "id": {"type": "string"}
    },
    "required": ["videos"],
    "additionalProperties": False
})
@queue_task_wrapper(bypass_queue=False)
def repeat_videos(job_id, data):
    videos = data['videos']
    webhook_url = data.get('webhook_url')
    id = data.get('id')

    logger.info(f"Job {job_id}: Received repeat-videos request for {len(videos)} videos")

    try:
        gcs_url = process_videos_repetition(videos, job_id)
        logger.info(f"Job {job_id}: Video repetition process completed successfully")

        return gcs_url, "/repeat-videos", 200
        
    except Exception as e:
        logger.error(f"Job {job_id}: Error during video repetition process - {str(e)}")
        return str(e), "/repeat-videos", 500