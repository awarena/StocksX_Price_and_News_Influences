from setuptools import setup, find_packages
import os
import re

def get_dependencies_from_conda():
    """Extract dependencies from conda environment file with robust error handling"""
    conda_deps = []
    pip_deps = []
    
    # Path to your environment.yml file
    env_file = os.path.join(os.path.dirname(__file__), 'tf270_stocks.yml')
    
    if os.path.exists(env_file):
        try:
            # Read file in binary mode to handle null bytes
            with open(env_file, 'rb') as f:
                content = f.read()
                
            # Remove null bytes
            content = content.replace(b'\x00', b'')
            
            # Decode to string
            content = content.decode('utf-8')
            
            # Extract conda dependencies (simplified approach)
            conda_section = re.search(r'dependencies:(.*?)(?:  - pip:|$)', content, re.DOTALL)
            if conda_section:
                conda_lines = conda_section.group(1).strip().split('\n')
                for line in conda_lines:
                    line = line.strip()
                    if line.startswith('- ') and not line.startswith('- _') and not line.startswith('- pip:'):
                        # Extract the package name without version
                        package = line[2:].split('=')[0].strip()
                        if package and package != 'python':
                            conda_deps.append(package)
            
            # Extract pip dependencies
            pip_section = re.search(r'- pip:(.*?)(?:$)', content, re.DOTALL)
            if pip_section:
                pip_lines = pip_section.group(1).strip().split('\n')
                for line in pip_lines:
                    line = line.strip()
                    if line.startswith('    - ') and not line.startswith('    - -e'):
                        # Extract the package name without version
                        package = line[6:].split('==')[0].strip()
                        if package:
                            pip_deps.append(package)
            
            print(f"Found {len(conda_deps)} conda dependencies and {len(pip_deps)} pip dependencies")
            
        except Exception as e:
            print(f"Error parsing environment file: {str(e)}")
            print("Using default dependencies instead")
            return [
                "pandas",
                "numpy",
                "pyspark",
                "pandas-market-calendars",
                "pyiceberg",
            ]
    else:
        print(f"Environment file not found at {env_file}")
        return [
            "pandas",
            "numpy", 
            "pyspark",
            "pandas-market-calendars", 
            "pyiceberg",
        ]
    
    # Combine conda and pip dependencies, removing duplicates
    all_deps = list(set(conda_deps + pip_deps))
    return all_deps

# Get the list of required package dependencies
dependencies = get_dependencies_from_conda()

# Print the dependencies to verify
print("\nPackage dependencies:")
for dep in dependencies:
    print(f"  - {dep}")
# Read version from __init__.py
with open('stocksx/__init__.py', 'r') as f:
    version = re.search(r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]', f.read()).group(1)


setup(
    name="stocksx",
    version=version,
    packages=find_packages(),
    install_requires=get_dependencies_from_conda(),
    python_requires=">=3.8",
    author="Alex Hoang",
    author_email="hvn0904@gmail.com",
    description="Stock price and news analysis pipeline using Apache Spark and Iceberg",
    keywords="stocks, finance, spark, iceberg",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Insurance Industry", 
        "Programming Language :: Python :: 3",
    ],
)