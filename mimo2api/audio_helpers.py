import json
import logging
import os
from typing import Any

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class AudioSpeechRequest(BaseModel):
    input: str = Field(min_length=1)
    model: str | None = None
    voice: str | None = None
    response_format: str = "wav"
    instructions: str | None = None


def map_openai_tts_voice(voice: str | None) -> str:
    default_voice_map = {
        "alloy": "mimo_default",
        "ash": "mimo_default",
        "ballad": "mimo_default",
        "coral": "mimo_default",
        "echo": "mimo_default",
        "fable": "mimo_default",
        "nova": "mimo_default",
        "onyx": "mimo_default",
        "sage": "mimo_default",
        "shimmer": "mimo_default",
        "verse": "mimo_default",
    }
    override_map_raw = os.getenv("MIMO_TTS_VOICE_MAP", "").strip()
    if override_map_raw:
        try:
            override_map = json.loads(override_map_raw)
            if isinstance(override_map, dict):
                default_voice_map.update({str(k): str(v) for k, v in override_map.items()})
        except json.JSONDecodeError:
            logger.warning("⚠️ MIMO_TTS_VOICE_MAP 不是合法 JSON，忽略自定义语音映射")

    if not voice:
        return "mimo_default"
    return default_voice_map.get(voice, voice)


def map_openai_tts_model(model: str | None) -> str:
    if not model:
        return "mimo-v2-tts"
    model_map = {
        "tts-1": "mimo-v2-tts",
        "tts-1-hd": "mimo-v2-tts",
        "gpt-4o-mini-tts": "mimo-v2-tts",
        "mimo-v2-tts": "mimo-v2-tts",
    }
    return model_map.get(model, "mimo-v2-tts")


def audio_media_type(audio_format: str) -> str:
    media_types = {
        "aac": "audio/aac",
        "flac": "audio/flac",
        "mp3": "audio/mpeg",
        "opus": "audio/ogg",
        "pcm": "audio/pcm",
        "wav": "audio/wav",
    }
    return media_types.get(audio_format.lower(), "application/octet-stream")


def pick_nested_value(data: Any, path: list[Any]) -> Any:
    current = data
    for key in path:
        if isinstance(key, int):
            if not isinstance(current, list) or key >= len(current):
                return None
            current = current[key]
        else:
            if not isinstance(current, dict):
                return None
            current = current.get(key)
            if current is None:
                return None
    return current


def extract_audio_payload(data: Any) -> tuple[str | None, str | None]:
    audio_path_pairs = [
        (["audio", "data"], ["audio", "format"]),
        (["data", "audio", "data"], ["data", "audio", "format"]),
        (["choices", 0, "message", "audio", "data"], ["choices", 0, "message", "audio", "format"]),
        (["choices", 0, "audio", "data"], ["choices", 0, "audio", "format"]),
        (["output", "audio", "data"], ["output", "audio", "format"]),
    ]

    for data_path, fmt_path in audio_path_pairs:
        audio_b64 = pick_nested_value(data, data_path)
        if isinstance(audio_b64, str) and audio_b64:
            audio_format = pick_nested_value(data, fmt_path)
            return audio_b64, audio_format if isinstance(audio_format, str) else None

    if isinstance(data, dict):
        audio = data.get("audio")
        if isinstance(audio, dict):
            audio_b64 = audio.get("data")
            audio_format = audio.get("format")
            if isinstance(audio_b64, str) and audio_b64:
                return audio_b64, audio_format if isinstance(audio_format, str) else None
        for value in data.values():
            audio_b64, audio_format = extract_audio_payload(value)
            if audio_b64:
                return audio_b64, audio_format
    elif isinstance(data, list):
        for item in data:
            audio_b64, audio_format = extract_audio_payload(item)
            if audio_b64:
                return audio_b64, audio_format

    return None, None
