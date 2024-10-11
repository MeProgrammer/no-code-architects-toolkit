import os
import ffmpeg
import requests
from services.file_management import download_file
from services.gcp_toolkit import upload_to_gcs, GCP_BUCKET_NAME, gcs_client  # Import gcs_client

# Set the default local storage directory
STORAGE_PATH = "/tmp/"

def process_conversion(media_url, job_id, bitrate='128k', webhook_url=None):
    """Convert media to MP3 format with specified bitrate."""
    input_filename = download_file(media_url, os.path.join(STORAGE_PATH, f"{job_id}_input"))
    output_filename = f"{job_id}.mp3"
    output_path = os.path.join(STORAGE_PATH, output_filename)

    try:
        # Convert media file to MP3 with specified bitrate
        (
            ffmpeg
            .input(input_filename)
            .output(output_path, acodec='libmp3lame', audio_bitrate=bitrate)
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
        os.remove(input_filename)
        print(f"Conversion successful: {output_path} with bitrate {bitrate}")

        # Ensure the output file exists locally before attempting upload
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after conversion.")

        return output_path

    except Exception as e:
        print(f"Conversion failed: {str(e)}")
        raise

def process_video_combination(media_urls, job_id, webhook_url=None):
    """Combine multiple videos into one."""
    input_files = []
    output_filename = f"{job_id}.mp4"
    output_path = os.path.join(STORAGE_PATH, output_filename)

    try:
        # Download all media files
        for i, media_item in enumerate(media_urls):
            url = media_item['video_url']
            input_filename = download_file(url, os.path.join(STORAGE_PATH, f"{job_id}_input_{i}"))
            input_files.append(input_filename)

        # Generate an absolute path concat list file for FFmpeg
        concat_file_path = os.path.join(STORAGE_PATH, f"{job_id}_concat_list.txt")
        with open(concat_file_path, 'w') as concat_file:
            for input_file in input_files:
                # Write absolute paths to the concat list
                concat_file.write(f"file '{os.path.abspath(input_file)}'\n")

        # Use the concat demuxer to concatenate the videos
        (
            ffmpeg.input(concat_file_path, format='concat', safe=0).
                output(output_path, c='copy').
                run(overwrite_output=True)
        )

        # Clean up input files
        for f in input_files:
            os.remove(f)
        os.remove(concat_file_path)  # Remove the concat list file after the operation

        print(f"Video combination successful: {output_path}")

        # Check if the output file exists locally before upload
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after combination.")

        # Upload to Google Drive or GCP Storage
        if GCP_BUCKET_NAME:
            uploaded_file_url = upload_to_gcs(output_path, GCP_BUCKET_NAME) 

        # If upload fails, log the failure
        if not uploaded_file_url:
            raise FileNotFoundError(f"Failed to upload the output file {output_path}")

        os.remove(output_path)

        return uploaded_file_url
    except Exception as e:
        print(f"Video combination failed: {str(e)}")
        raise 
    
def process_videos_repetition(videos, job_id, webhook_url=None):
    """Process multiple videos with their respective repeat counts and combine them."""
    input_files = []
    output_filename = f"{job_id}.mp4"
    output_path = os.path.join(STORAGE_PATH, output_filename)
    concat_file_path = os.path.join(STORAGE_PATH, f"{job_id}_concat_list.txt")

    try:
        # Download all videos
        for i, video_item in enumerate(videos):
            url = video_item['video_url']
            input_filename = download_file(url, os.path.join(STORAGE_PATH, f"{job_id}_input_{i}"))
            input_files.append({
                'path': input_filename,
                'repeat_count': video_item['repeat_count']
            })

        # Generate concat list file with repeated videos
        with open(concat_file_path, 'w') as concat_file:
            for input_file in input_files:
                # Write the same file path multiple times based on repeat_count
                for _ in range(input_file['repeat_count']):
                    concat_file.write(f"file '{os.path.abspath(input_file['path'])}'\n")

        # Use the concat demuxer to combine all videos
        (
            ffmpeg.input(concat_file_path, format='concat', safe=0)
            .output(output_path, c='copy')
            .run(overwrite_output=True)
        )

        print(f"Video repetition and combination successful: {output_path}")

        # Clean up input files
        for input_file in input_files:
            os.remove(input_file['path'])
        os.remove(concat_file_path)

        # Check if the output file exists locally before upload
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Output file {output_path} does not exist after processing.")

        # Upload to GCP Storage
        if GCP_BUCKET_NAME:
            uploaded_file_url = upload_to_gcs(output_path, GCP_BUCKET_NAME)

        # If upload fails, log the failure
        if not uploaded_file_url:
            raise FileNotFoundError(f"Failed to upload the output file {output_path}")

        os.remove(output_path)

        return uploaded_file_url

    except Exception as e:
        # Clean up any remaining files in case of error
        for input_file in input_files:
            if os.path.exists(input_file['path']):
                os.remove(input_file['path'])
        if os.path.exists(concat_file_path):
            os.remove(concat_file_path)
        if os.path.exists(output_path):
            os.remove(output_path)
        print(f"Video repetition and combination failed: {str(e)}")
        raise