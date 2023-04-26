
import pyaudio, wave
pa = pyaudio.PyAudio()
"""
check api and devices with api-device.py
"""

#wav_file = wave.open('audio-clip.wav')
wav_file = wave.open('audio-recording.wav')
stream_out = pa.open(
    rate=wav_file.getframerate(),     # sampling rate
    channels=wav_file.getnchannels(), # number of output channels
    format=pa.get_format_from_width(wav_file.getsampwidth()),  # sample format and length
    output=True,             # output stream flag
    output_device_index=8,   # output device index
    #output_device_index=20,   # output device index
    #output_device_index=4,   # output device index
    frames_per_buffer=1024,  # buffer length
)
output_audio = wav_file.readframes(5 * wav_file.getframerate())
stream_out.write(output_audio)
