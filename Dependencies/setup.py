import sys
import subprocess
import os

def setup():
    # grabbing the requirements text next to the script
    requirements_txt = os.path.join(os.path.dirname(__file__), 'requirements.txt')

    # open and read packages that are needed.
    with open(requirements_txt, 'r', encoding='UTF-8') as file:
        data = file.read()
        modules = data.split('\n')
    print('-'*20)
    print('These Packages are needed:')
    print(*modules, sep='\n')

    # list all packages installed through pip in freeze format
    stream = os.popen('pip list --user --format=freeze')
    Package = stream.read()
    pip_list = list(Package.split("\n"))

    # loop through required modules, if they are in the pip_list skip, if not, install
    for package in modules:
        if package not in pip_list:
            subprocess.check_call([sys.executable, '-m', 'pip', 'install', f'{package.split("==")[0]}[full]=={package.split("==")[-1]}', '--user'])
            print(f'Installed: {package}')
        else:
            print(f'{package} is already installed')
if __name__ == '__main__':
    setup()
        