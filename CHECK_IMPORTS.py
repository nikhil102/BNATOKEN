import os
import importlib
import platform

system = platform.system()
required_modules = []
if system == "Windows":
    required_modules = ['urllib.request', 'json', 'pandas', 'datetime', 'platform', 'gzip', 'requests', 'io','cerberus','pprint']
elif system == "Linux":
    required_modules = ['urllib.request', 'json', 'pandas', 'datetime', 'platform', 'gzip', 'requests', 'io','pycurl','cerberus','pprint']
else:
    print(f"Running on {system}")
# List of required modules

def check_modules():
    i = 0
    for module in required_modules:
        try:
            importlib.import_module(module)
            print(f"{i} {module} Is Installed.")
            print("__"*20)
        except ImportError:
            print(f"{i} {module} Is NOT Installed.")
            print("__"*20)
        i = i + 1
        
if __name__ == "__main__":
    check_modules()