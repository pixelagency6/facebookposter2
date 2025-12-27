import os
import sys
import math
import logging
import asyncio
import subprocess
import shutil
import time
from typing import Dict, Optional, List, Union, Any
from dataclasses import dataclass, field
from enum import Enum

import imageio_ffmpeg
import requests
from aiohttp import web
from pyrogram import Client, filters
from pyrogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton, 
    Message, 
    CallbackQuery
)

# --- Configuration & Environment ---

@dataclass
class Config:
    """Centralized configuration management."""
    API_ID: int
    API_HASH: str
    BOT_TOKEN: str
    FB_PAGE_TOKEN: str
    FB_PAGE_ID: str
    PORT: int = 8080
    FFMPEG_BIN: str = "ffmpeg"

    @classmethod
    def load(cls) -> "Config":
        """Loads and validates environment variables."""
        # Setup FFmpeg path for Render.com or local
        ffmpeg_bin = os.getenv("FFMPEG_BINARY")
        if not ffmpeg_bin:
            # Fallback to imageio's binary if system ffmpeg is missing
            try:
                ffmpeg_bin = imageio_ffmpeg.get_ffmpeg_exe()
            except Exception:
                ffmpeg_bin = "ffmpeg"
        
        # Validate critical vars
        try:
            config = cls(
                API_ID=int(os.getenv("API_ID", "0")),
                API_HASH=os.getenv("API_HASH", ""),
                BOT_TOKEN=os.getenv("BOT_TOKEN", ""),
                FB_PAGE_TOKEN=os.getenv("FB_TOKEN", ""),
                FB_PAGE_ID=os.getenv("FB_PAGE_ID", ""),
                PORT=int(os.getenv("PORT", "8080")),
                FFMPEG_BIN=ffmpeg_bin
            )
            print(f"✅ Bot Config Loaded. FFmpeg path: {ffmpeg_bin}")
            return config
        except ValueError as e:
            logging.critical(f"Configuration Error: {e}")
            sys.exit(1)

# Initialize Config
CONFIG = Config.load()

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("BotCore")

# --- Constants & Enums ---
class UploadMode(Enum):
    BULK = "bulk"
    CUSTOM = "custom"
    SPLIT = "split"

class Step(Enum):
    WAITING_TITLE = "title"
    WAITING_DESC = "desc"
    WAITING_VIDEO = "video"
    IDLE = "idle"

@dataclass
class UserState:
    mode: UploadMode = UploadMode.BULK
    step: Step = Step.WAITING_VIDEO
    meta_data: Dict[str, Any] = field(default_factory=dict)

# --- State Management ---
class StateManager:
    """
    Abstracts state management. 
    Currently in-memory, but designed to be easily swapped for Redis.
    """
    def __init__(self):
        self._db: Dict[int, UserState] = {}

    def get(self, user_id: int) -> UserState:
        if user_id not in self._db:
            self._db[user_id] = UserState()
        return self._db[user_id]

    def update(self, user_id: int, **kwargs):
        state = self.get(user_id)
        for key, value in kwargs.items():
            if hasattr(state, key):
                setattr(state, key, value)

state_manager = StateManager()

# --- Services ---

class VideoProcessor:
    """Handles video manipulation using direct FFmpeg subprocess calls."""
    
    @staticmethod
    def get_duration(input_path: str) -> float:
        """Get video duration using ffprobe."""
        cmd = [
            CONFIG.FFMPEG_BIN, "-i", input_path, "-hide_banner"
        ]
        # FFmpeg outputs duration to stderr
        result = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.DEVNULL, text=True)
        
        # Parse output for "Duration: 00:00:00.00"
        try:
            import re
            match = re.search(r"Duration: (\d{2}):(\d{2}):(\d{2}\.\d{2})", result.stderr)
            if match:
                hours, mins, secs = map(float, match.groups())
                return hours * 3600 + mins * 60 + secs
        except Exception as e:
            logger.error(f"Failed to parse duration: {e}")
        return 0.0

    @staticmethod
    def split_video(input_path: str, output_dir: str, chunk_length: int = 60) -> List[str]:
        """
        Splits video into chunks using FFmpeg stream copying (fast) or re-encoding.
        Returns a list of generated file paths.
        """
        duration = VideoProcessor.get_duration(input_path)
        if duration <= 0:
            raise ValueError("Could not determine video duration.")

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        generated_files = []
        total_parts = math.ceil(duration / chunk_length)
        
        # Segment muxer is much more stable and faster than loop-splitting
        output_pattern = os.path.join(output_dir, "part_%03d.mp4")
        
        logger.info(f"Splitting video of {duration}s into {total_parts} parts via FFmpeg segment muxer.")
        
        # Command: Re-encode to ensure Facebook compatibility (aac/h264) but fast
        cmd = [
            CONFIG.FFMPEG_BIN,
            "-i", input_path,
            "-c:v", "libx264", "-preset", "veryfast", "-crf", "23", # Good balance of speed/quality
            "-c:a", "aac", "-b:a", "128k",
            "-f", "segment",
            "-segment_time", str(chunk_length),
            "-reset_timestamps", "1",
            output_pattern
        ]

        try:
            subprocess.run(cmd, check=True, stderr=subprocess.PIPE)
            
            # Gather generated files
            for file in sorted(os.listdir(output_dir)):
                if file.startswith("part_") and file.endswith(".mp4"):
                    generated_files.append(os.path.join(output_dir, file))
                    
            return generated_files
        except subprocess.CalledProcessError as e:
            logger.error(f"FFmpeg Error: {e.stderr.decode()}")
            raise RuntimeError("FFmpeg processing failed.")

class FacebookService:
    """Handles interactions with Facebook Graph API."""
    
    BASE_URL = "https://graph-video.facebook.com/v18.0"

    @staticmethod
    def upload_video(video_path: str, description: str, title: Optional[str] = None) -> Dict:
        if not os.path.exists(video_path):
            return {'error': {'message': 'File not found locally'}}

        url = f"{FacebookService.BASE_URL}/{CONFIG.FB_PAGE_ID}/videos"
        
        payload = {
            'access_token': CONFIG.FB_PAGE_TOKEN,
            'description': description
        }
        if title:
            payload['title'] = title

        # Getting file size for logging
        file_size_mb = os.path.getsize(video_path) / (1024 * 1024)
        logger.info(f"Uploading {os.path.basename(video_path)} ({file_size_mb:.2f} MB)...")

        try:
            with open(video_path, 'rb') as file:
                files = {'source': file}
                # Increased timeout for large files
                response = requests.post(url, data=payload, files=files, timeout=300)
                return response.json()
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return {'error': {'message': str(e)}}

# --- Async Workers ---

async def process_split_and_upload(client: Client, message: Message, video_path: str, caption: str):
    """Orchestrates the split and upload workflow."""
    status_msg = await message.reply_text("✂️ **FFmpeg Engine:** Analyzing and splitting video...")
    temp_dir = f"temp_{message.id}"
    
    try:
        loop = asyncio.get_running_loop()
        
        # 1. Split Video (CPU Bound - run in executor)
        video_parts = await loop.run_in_executor(
            None, 
            VideoProcessor.split_video, 
            video_path, 
            temp_dir
        )
        
        if not video_parts:
            await status_msg.edit_text("❌ Failed to split video.")
            return

        total_parts = len(video_parts)
        await status_msg.edit_text(f"✅ Split into {total_parts} parts.\n🚀 Starting upload queue...")

        # 2. Upload Parts (I/O Bound)
        results = []
        
        # Semaphore prevents too many concurrent uploads (memory/bandwidth protection)
        sem = asyncio.Semaphore(2) 

        async def upload_worker(idx: int, path: str):
            async with sem:
                part_title = f"Part {idx}/{total_parts}"
                part_desc = f"{caption}\n\n({part_title})"
                
                # Update UI periodically (optional, simplified here)
                res = await loop.run_in_executor(
                    None, 
                    FacebookService.upload_video, 
                    path, part_desc, part_title
                )
                return (idx, res)

        tasks = [upload_worker(i+1, path) for i, path in enumerate(video_parts)]
        upload_results = await asyncio.gather(*tasks)

        # 3. Compile Report
        success_count = 0
        report_lines = []
        
        for idx, res in upload_results:
            if 'id' in res:
                success_count += 1
                report_lines.append(f"✅ Part {idx}: [Link](https://fb.com/{res['id']})")
            else:
                err = res.get('error', {}).get('message', 'Unknown')
                report_lines.append(f"❌ Part {idx}: {err}")

        final_text = (
            f"🏁 **Batch Complete**\n"
            f"Success: {success_count}/{total_parts}\n\n" + 
            "\n".join(report_lines)
        )
        
        # Check text length limit
        if len(final_text) > 4000:
            final_text = final_text[:4000] + "... (truncated)"
            
        await status_msg.edit_text(final_text, disable_web_page_preview=True)

    except Exception as e:
        logger.error(f"Process Error: {e}", exc_info=True)
        await status_msg.edit_text(f"❌ Critical Error: {str(e)}")
    
    finally:
        # Cleanup
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        if os.path.exists(video_path):
            os.remove(video_path)

# --- Bot Client & Handlers ---

app = Client("senior_bot", api_id=CONFIG.API_ID, api_hash=CONFIG.API_HASH, bot_token=CONFIG.BOT_TOKEN)

@app.on_message(filters.command("start"))
async def start_handler(client, message: Message):
    user_name = message.from_user.first_name
    
    # Reset State
    state_manager.update(
        message.chat.id, 
        mode=UploadMode.BULK, 
        step=Step.WAITING_VIDEO, 
        meta_data={}
    )

    text = (
        f"👋 **Hello {user_name}!**\n\n"
        "**Select Mode:**\n"
        "🚀 **Bulk:** Immediate upload.\n"
        "📝 **Custom:** Set Title/Desc manually.\n"
        "✂️ **Split:** Auto-split long videos (1 min)."
    )
    
    buttons = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🚀 Bulk", callback_data="set_mode_bulk"),
            InlineKeyboardButton("📝 Custom", callback_data="set_mode_custom")
        ],
        [InlineKeyboardButton("✂️ Split Series", callback_data="set_mode_split")]
    ])
    
    await message.reply_text(text, reply_markup=buttons)

@app.on_callback_query(filters.regex(r"^set_mode_"))
async def mode_handler(client, callback: CallbackQuery):
    mode_str = callback.data.split("_")[-1]
    chat_id = callback.message.chat.id
    
    new_mode = UploadMode(mode_str)
    
    if new_mode == UploadMode.CUSTOM:
        state_manager.update(chat_id, mode=new_mode, step=Step.WAITING_TITLE)
        msg = "📝 **Custom Mode Activated**\nPlease send the **TITLE** for your next video."
    elif new_mode == UploadMode.SPLIT:
        state_manager.update(chat_id, mode=new_mode, step=Step.WAITING_VIDEO)
        msg = "✂️ **Split Mode Activated**\nSend a long video, I will slice it."
    else:
        state_manager.update(chat_id, mode=new_mode, step=Step.WAITING_VIDEO)
        msg = "🚀 **Bulk Mode Activated**\nSend videos to upload instantly."
        
    await callback.message.edit_text(msg)

@app.on_message(filters.text & filters.private)
async def text_handler(client, message: Message):
    state = state_manager.get(message.chat.id)
    
    if state.mode == UploadMode.CUSTOM:
        if state.step == Step.WAITING_TITLE:
            state.meta_data['title'] = message.text
            state.step = Step.WAITING_DESC
            await message.reply_text("✅ Title saved. Now send the **DESCRIPTION**.")
            
        elif state.step == Step.WAITING_DESC:
            state.meta_data['desc'] = message.text
            state.step = Step.WAITING_VIDEO
            await message.reply_text(f"✅ Description saved.\n🎥 **Send Video Now.**")
            
    else:
        # Ignore random text in other modes
        pass

@app.on_message(filters.video & filters.private)
async def video_handler(client, message: Message):
    chat_id = message.chat.id
    state = state_manager.get(chat_id)
    
    # Validation
    if state.mode == UploadMode.CUSTOM and state.step != Step.WAITING_VIDEO:
        await message.reply_text("⚠️ Please finish setting Title/Description first.")
        return

    status_msg = await message.reply_text("📥 Downloading media...")
    file_path = ""

    try:
        file_path = await message.download()
        
        if state.mode == UploadMode.SPLIT:
            # Hand off to complex logic
            caption = message.caption or "Series Upload"
            await status_msg.delete()
            await process_split_and_upload(client, message, file_path, caption)
            return

        # Direct Upload Handling (Bulk or Custom)
        await status_msg.edit_text("📤 Uploading to Facebook...")
        
        title = state.meta_data.get('title')
        desc = state.meta_data.get('desc') or message.caption or "Uploaded via Bot"
        
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(
            None, 
            FacebookService.upload_video, 
            file_path, desc, title
        )

        if 'id' in result:
            await status_msg.edit_text(f"✅ **Published!**\nID: `{result['id']}`")
            # Reset Custom mode to Bulk after success to prevent mistakes
            if state.mode == UploadMode.CUSTOM:
                state_manager.update(chat_id, mode=UploadMode.BULK, step=Step.WAITING_VIDEO, meta_data={})
                await message.reply_text("🔄 Mode reset to Bulk.")
        else:
            err = result.get('error', {}).get('message', 'Unknown API Error')
            await status_msg.edit_text(f"❌ Upload Failed: {err}")

    except Exception as e:
        logger.error(f"Handler Error: {e}", exc_info=True)
        await status_msg.edit_text("❌ An internal error occurred.")
        
    finally:
        # File cleanup for non-split modes (split mode cleans up itself)
        if state.mode != UploadMode.SPLIT and file_path and os.path.exists(file_path):
            os.remove(file_path)

# --- Web Server (Health Check) ---

async def health_check(request):
    return web.Response(text="Running", status=200)

async def start_web_server():
    web_app = web.Application()
    web_app.add_routes([web.get('/', health_check)])
    
    app_runner = web.AppRunner(web_app)
    await app_runner.setup()
    
    site = web.TCPSite(app_runner, "0.0.0.0", CONFIG.PORT)
    await site.start()
    logger.info(f"Web server started on port {CONFIG.PORT}")

# --- Entry Point ---

if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(start_web_server())
        logger.info("Bot starting...")
        app.run()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.critical(f"Fatal Startup Error: {e}")
