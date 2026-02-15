import os
import subprocess
import sys

def run(command):
    subprocess.check_call(command, shell=True)

def main():
    if not os.path.exists(".venv"):
        print("Creating virtual environment...")
        run(f"{sys.executable} -m venv .venv")

    if os.name == "nt":
        activate = ".venv\\Scripts\\activate"
        python_bin = ".venv\\Scripts\\python"
        pip_bin = ".venv\\Scripts\\pip"
    else:
        activate = "source .venv/bin/activate"
        python_bin = ".venv/bin/python"
        pip_bin = ".venv/bin/pip"

    print("Upgrading pip...")
    run(f"{pip_bin} install --upgrade pip")

    print("Installing requirements...")
    run(f"{pip_bin} install -r requirements.txt")

    print("Starting Streamlit app...")
    run(f"{python_bin} -m streamlit run frontend/poi_ingestion_app.py")

if __name__ == "__main__":
    main()
