import pyaudio
import pprint 
pa = pyaudio.PyAudio()
print("default-host_api:\n", pa.get_default_host_api_info())
print("all api's\n")
for id in range(pa.get_host_api_count()):
    print("host_api:", pa.get_host_api_info_by_index(id))

print("all devices \n")
print("default-output-device:\n", pa.get_default_output_device_info())

print("\n")
for id in range(pa.get_device_count()):
  dev_dict = pa.get_device_info_by_index(id)
  print(f"id:{id}\n")
  for key, value in dev_dict.items():
      print("key:value = ", key, value)


