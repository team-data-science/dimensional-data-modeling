# Dimensional data modeling
## Prerequisites
### Option 1: Setup with Anaconda (Conda)
Configure a modelingcourse environment with Anaconda, an open source package management system.

1. Install miniconda on your machine following the instructions:
<br/>Linux: https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html
<br/>Mac: https://docs.conda.io/projects/conda/en/latest/user-guide/install/macos.html
<br/>Windows: https://docs.conda.io/projects/conda/en/latest/user-guide/install/windows.html

2. Clone the github repository.
```
git clone https://github.com/team-data-science/dimensional-data-modeling.git
cd dimensional-data-modeling/assets_scripts
```

3. Running this command will create a new conda environment that is provisioned with most of the libraries you need for this project.

```
conda env create -f modelingenv.yml
```

4. Verify that the modelingcourse environment was created in your environments:

```
conda info --envs
```

5. Cleanup downloaded libraries (remove tarballs, zip files, etc):

```
conda clean -tp
```

6. To activate the environment:
<br/>OS X and Linux: ```$ source activate modelingcourse```
<br/>Windows:```$ source activate modelingcourse```

### Option 2: Setup with Python and venv (without Conda)

Alternatively, you can set up the environment using `python3 -m venv` and `pip` to install the dependencies. Follow the steps below:

1. Make sure you have Python 3.10 installed on your machine.

2. **For Linux users**, ensure that the `venv` module is installed, `pkg-config` and MySQL development libraries. Run the following command:
<br/>```sudo apt update```
<br/>```sudo apt install python3-venv -y```
<br/>```sudo apt install pkg-config libmysqlclient-dev build-essential -y```

3. Clone the GitHub repository:

```bash
git clone https://github.com/team-data-science/dimensional-data-modeling.git
cd dimensional-data-modeling/assets_scripts
```

4. Create a new virtual environment using `venv`:

```
python3 -m venv modelingenv
```

5. To activate the environment:
<br/>OS X and Linux: ```source modelingenv/bin/activate```
<br/>Windows:```modelingenv\Scripts\activate```

6. Install the dependencies listed in `requirements.txt` using `pip`. The `requirements.txt` file is located in the `assets_scripts` directory:

```
pip install -r assets_scripts/requirements.txt
```