
import pyaudio, wave
pa = pyaudio.PyAudio()
pa.get_default_host_api_info()

stream_in = pa.open(
    rate=48000,
    channels=2,
    format=pyaudio.paInt16,
    input=True,                   # input stream flag
    input_device_index=0,         # input device index
    frames_per_buffer=1024
)

# read 5 seconds of the input stream
input_audio = stream_in.read(5 * 48000)

output_filename = 'audio-recording.wav'
wav_file = wave.open(output_filename, 'wb')

# define audio stream properties
wav_file.setnchannels(2)        # number of channels
wav_file.setsampwidth(2)        # sample width in bytes
wav_file.setframerate(48000)    # sampling rate in Hz

# write samples to the file
wav_file.writeframes(input_audio)


